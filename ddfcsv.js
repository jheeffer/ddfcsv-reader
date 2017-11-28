let ddfcsvReader = {
	
	path: null,
	keyValueLookup: null,
	resourcesLookup: null,
	datapackage: null,

	loadDataset(path) {
		if (!path.endsWith("datapackage.json")) {
			if (!path.endsWith("/")) {
				path = path + "/";
			}
			path = path + "datapackage.json";
		}
		this.datapackagePath = path;
		this.basePath = this.getBasePath(path);
		return this.readDataPackage(path);
	},

	getBasePath(datapackagePath = this.path) {
		datapackagePath = datapackagePath.split("/");
		datapackagePath.pop();
		return datapackagePath.join("/") + "/";
	},

	readDataPackage(path) {
		const dataPackagePromise = fetch(path)
			.then(r => r.json())
			.then(this.handleNewDatapackage.bind(this));
		return dataPackagePromise;
	},

	handleNewDatapackage(dp) {
		this.datapackage = dp;
		this.buildResourcesLookup();
		this.buildKeyValueLookup();
		return this.performQuery({ select: { key: ["concept"], value: ["concept_type", "domain"] }, from: "concepts" }).then(result => {
			result = result
				.filter(concept => concept.concept_type == "entity_set")
				.map(concept => ({ concept: "is--" + concept.concept, concept_type: "boolean", domain: null }))
				.concat(result);
			this.concepts = result;
			this.conceptsLookup = this.buildDataLookup(this.concepts, "concept");
			return dp;
		});
	},

	buildDataLookup(data, key) {
		return new Map(data.map(row => [row[key], row]));
	},

	performQuery(query) {
		const { select, from="", where = {}, join = {}, order } = query;
		// schema queries can be answered synchronously (if datapackage is loaded)
		if (from.split(".")[1] == "schema") 
			return Promise.resolve(this.getSchemaResponse(query))

		// load join lists
		const joinsPromise = this.getJoinLists(join, query);

		// rest async
		return new Promise((resolve, reject) => {
			const resources = this.getResources(select.key, select.value);
			Promise.all([...resources].map(this.loadResource.bind(this)))
				.then(responses => {
					let result = responses.reduce((result, resourceResponse) => {
						const projected = this.applyProjectionAndRenameHeader(resourceResponse, select);
						return this.joinData(select.key, result, projected);
					}, []);
					this.fillMissingValues(result, select);
					this.getEntityFilter(select.key).then(entityFilter => {
						joinsPromise.then(joinLists => {
							const filter = this.mergeFilters(entityFilter, where);
							result = this.applyFilter(result, filter, joinLists);
							resolve(result);
						});
					});
				})        
		});
	},

	mergeFilters(...filters) {
		return filters.reduce((a,b) => { a["$and"].push(b); return a }, { "$and": [] });
	},

	getSchemaResponse(query) {
		const collection = query.from.split('.')[0];
		const datapackageToSchemaResponse = kvPair => ({ key: kvPair.primaryKey, value: kvPair.value });
		if (this.datapackage.ddfSchema[collection]) {
			return this.datapackage.ddfSchema[collection].map(datapackageToSchemaResponse);
		} else if (collection == "*") {
			return Object.keys(this.datapackage.ddfSchema)
				.map(collection => this.datapackage.ddfSchema[collection]
					.map(datapackageToSchemaResponse))
				.reduce((a,b) => a.concat(b));
		} else {
			this.throwError("No valid collection (" + collection + ") for schema query");
		}
	},

	fillMissingValues(data, select) {
		const projection = select.key.concat(select.value);
		data.forEach(row => {
			for(let field of projection) {
				if (typeof row[field] == "undefined") row[field] = null;
			}
		});
	},

	getOperator(op) {
		const ops = {
			/* logical operatotrs */
			$and: (row, predicates, q) => predicates.map(p => this.applyFilterRow(row,p,q)).reduce((a,b) => a && b),
			$or:  (row, predicates, q) => predicates.map(p => this.applyFilterRow(row,p,q)).reduce((a,b) => a || b),
			$not: (row, predicates, q) => !this.applyFilterRow(row, predicate, q),
			$nor: (row, predicates, q) => !predicates.map(p => this.applyFilterRow(row,p,q)).reduce((a,b) => a || b),

			/* equality operators */
			$eq:  (rowValue, filterValue) => rowValue == filterValue,
			$ne:  (rowValue, filterValue) => rowValue != filterValue,
			$gt:  (rowValue, filterValue) => rowValue > filterValue,
			$gte: (rowValue, filterValue) => rowValue >= filterValue,
			$lt:  (rowValue, filterValue) => rowValue < filterValue,
			$lte: (rowValue, filterValue) => rowValue <= filterValue,
			$in:  (rowValue, filterValue) => filterValue.includes(rowValue),
			$nin: (rowValue, filterValue) => !filterValue.includes(rowValue)
		}
		return ops[op] || null;
	},

	applyFilter(data, filter = {}, joinLists = {}) {
		return data.filter((row) => this.applyFilterRow(row, filter, joinLists));
	},

	applyFilterRow(row, filter, joinLists) {
		return Object.keys(filter).reduce((result, filterKey) => {
			if (operator = this.getOperator(filterKey)) {
				// apply operator
				return result && operator(row, filter[filterKey], joinLists);
			} else if(typeof filter[filterKey] == "string") { 
				
				// join entities or time
				if (filter[filterKey][0] == "$")
					return result && this.getOperator("$in")(row[filterKey], joinLists[filter[filterKey]]);
				
				// { <field>: <value> } is shorthand for { <field>: { $eq: <value> }} 
				else 
					return result && this.getOperator("$eq")(row[filterKey], filter[filterKey])
			
			} else {
				// go one step deeper - doesn't happen yet with DDFQL queries as fields have no depth
				return result && this.applyFilterRow(row[filterKey], filter[filterKey], joinLists);
			}
		}, true);
	},

	getJoinLists(join) {
		return Promise.all(Object.keys(join).map(joinID => this.getJoinList(joinID, join[joinID])))
			.then(results => results.reduce(this.mergeObjects, {}));
	},

	getJoinList(joinID, join) {
		const values = this.getFilterFields(join.where);
		return this.performQuery({ select: { key: [join.key], value: values }, where: join.where })
			.then(result => ({ [joinID]: result.map(row => row[join.key]) }));
	},

	getFilterFields(filter) {
		const fields = [];
		for (field in filter) {
			if (this.getOperator(field) && Array.isArray(filter[field]))
				filter[field].map(this.getFilterFields.bind(this)).forEach(subFields => fields.push(...subFields))
			else
				fields.push(field);
		};
		return fields;
	},

	mergeObjects: (a,b) => Object.assign(a,b),

	/**
	 * Filter concepts by type
	 * @param  {Array} conceptStrings   Array of concept strings to filter out. Default all concepts.
	 * @param  {Array} concept_types    Array of concept types to filter out
	 * @return {Array}                  Array of concept strings only of given types
	 */
	filterConceptsByType(conceptStrings = [...this.conceptLookup.keys()], concept_types) {
		return conceptStrings
			.filter(conceptString => this.conceptsLookup && concept_types.includes(this.conceptsLookup.get(conceptString).concept_type))
			.map(conceptString => this.conceptsLookup.get(conceptString));
	},

	/**
	 * Find the aliases an entity concept can have
	 * @param  {Array} conceptStrings An array of concept strings for which entity aliases are found if they're entity concepts
	 * @return {Map}                  Map with all aliases as keys and the entity concept as value
	 */
	getEntityConceptRenameMap(conceptStrings) {
		return this.filterConceptsByType(conceptStrings, ["entity_set", "entity_domain"])
			.map(concept => this.concepts
				.filter(lookupConcept => {  
					if (concept.concept_type == "entity_set")
						return lookupConcept.concept != concept.concept && // not the actual concept
							(
								lookupConcept.domain == concept.domain ||  // other entity sets in entity domain
								lookupConcept.concept == concept.domain    // entity domain of the entity set
							) 
					else // concept_type == "entity_domain"
						return lookupConcept.concept != concept.concept && // not the actual concept
							lookupConcept.domain == concept.concept          // entity sets of the entity domain
				})
				.reduce((map, aliasConcept) => map.set(aliasConcept.concept, concept.concept), new Map())
			).reduce((mapA, mapB) => new Map([...mapA,...mapB]), new Map())
	},

	/**
	 * Get a "$in" filter containing all entities for a entity concept.
	 * @param  {Array} conceptStrings Array of concept strings for which entities should be found
	 * @return {Array}                Array of filter objects for each entity concept
	 */
	getEntityFilter(conceptStrings) {
		const promises = this.filterConceptsByType(conceptStrings, ["entity_set"])
			.map(concept => this.performQuery({ select: { key: [concept.domain], value: ["is--" + concept.concept] } })
				.then(result => ({ [concept.concept]: 
						{ "$in": result
							.filter(row => row["is--" + concept.concept])
							.map(row => row[concept.domain])
						}
				}))
			);    
		
		return Promise.all(promises).then(results => {
			return results.reduce((a,b) => Object.assign(a,b),{});
		})
	},

	/**
	 * Returns all resources for a certain key value pair or multiple values for one key
	 * @param  {Array} key          The key of the requested resources
	 * @param  {Array/string} value The value or values found in the requested resources
	 * @return {Array}              Array of resource objects
	 */
	getResources(key, value) {
		if (!value || value.length == 0) {
			return new Set(
				[...this.keyValueLookup
					.get(this.createKeyString(key))
					.values()
				].reduce((a,b) => a.concat(b))
			)
		}
		if (Array.isArray(value)) {
			return value
				.map(singleValue => this.getResources(key,singleValue))
				.reduce((resultSet,resources) => new Set([...resultSet,...resources]), new Set());
		}
		return new Set(
			this.keyValueLookup
				.get(this.createKeyString(key))
				.get(value)
		);
	},

	/**
	 * Applies a projection to data and renames headers according to a entity concept rename map.
	 * @param  {Response object} resourceResponse Response object from resource query
	 * @param  {Select clause object} projection  Object with key and value arrays for respective projections
	 * @return {2d array}                         Data from resourceResponce with projection and renaming applied
	 */
	applyProjectionAndRenameHeader(resourceResponse, projection) {
		const renameMap = this.getEntityConceptRenameMap(projection.key);
		const projectionSets = { key: new Set(projection.key), value: new Set(projection.value) };
		const pk = new Set(resourceResponse.resource.schema.primaryKey);
		return resourceResponse.data.map(row => {
			const result = {};
			for (concept in row) {
				// following needs to happen in one loop as renaming can create duplicate column headers
				// if projection isn't applied directly.
				// E.g. a csv file with `<geo>,name,region` fields. Region is an entity set of domain geo.
				// { select: { key: ["region"], value: ["name"] } } is queried
				// After only rename the file would have headers `<region>,name,region`. 
				// This would be invalid and make unambiguous projection impossible.
				// Thus we need to apply projection right away with result: `<region>,name`

				// concept is in key: check for renaming
				if (pk.has(concept)) {
					const targetConcept = renameMap.get(concept) || concept;
					result[targetConcept] = row[concept];
				}
				// concept is in value: will not interfere with renamed keys
				if (projectionSets.value.has(concept)) {
					result[concept] = row[concept];
				}
			}
			return result;
		});
	},

	joinData(key, ...data) {

		const dataMap = data.reduce((result, data) => {
			data.forEach(row => {
				const keyString = this.createKeyString(key.map(keyPart => row[keyPart]));
				if (result.has(keyString)) {
					const resultRow = result.get(keyString);
					Object.assign(resultRow, row);
					/* Alternative for line above: with JOIN error detection
					for (concept in row) {
						if (resultRow[concept] && resultRow[concept] != row[concept]) {
							this.throwError("JOIN Error: two resources have different data for same key-value pair.");
						} else {
							resultRow[concept] = row[concept];
						}
					}
					*/
				} else {
					result.set(keyString, row)
				}
			})
			return result;
		}, new Map());
		return [...dataMap.values()];
	},

	throwError(error) {
		console.error(error);
	},

	createKeyString(keyArray) {
		return keyArray.slice(0).sort().join(",");
	},

	loadResource(resource) {
		const _this = this;
		if (resource.dataPromise) return resource.dataPromise;
		resource.dataPromise = new Promise((resolve, reject) => {
			Papa.parse(this.basePath + resource.path, {
				download: true,
				header: true,
				skipEmptyLines: true,
				dynamicTyping: function(headerName) {
					if (!_this.conceptsLookup) return true;
					const concept = _this.conceptsLookup.get(headerName) || {};
					return ["boolean", "measure"].includes(concept.concept_type);
				},
				complete: result => { 
					resource.response = result; 
					resource.data = result.data;
					result.resource = resource; 
					resolve(result);
				},
				error: error => reject(error)
			})
		});
		return resource.dataPromise;
	},

	buildKeyValueLookup() {
		if (this.keyValueLookup) return this.keyValueLookup;
		this.keyValueLookup = new Map();
		for (let collection in this.datapackage.ddfSchema) {
			this.datapackage.ddfSchema[collection].map(kvPair => {
				const key = this.createKeyString(kvPair.primaryKey);
				const resources = kvPair.resources.map(resourceName => this.resourcesLookup.get(resourceName));
				if (this.keyValueLookup.has(key)) {
					this.keyValueLookup.get(key).set(kvPair.value, resources);
				} else {
					this.keyValueLookup.set(key, new Map([[kvPair.value, resources]]));
				}
			})
		};
		return this.keyValueLookup;
	},


	buildResourcesLookup() {
		if (this.resourcesLookup) return this.resourcesLookup;
		this.datapackage.resources.forEach(resource => { 
			if (!Array.isArray(resource.schema.primaryKey)) {
				resource.schema.primaryKey = [resource.schema.primaryKey];
			}
		});
		this.resourcesLookup = new Map(this.datapackage.resources.map(resource => [resource.name, resource]));
		return this.resourcesLookup;
	}
}