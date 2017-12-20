let ddfcsvReader = {
	
	keyValueLookup: null,
	resourcesLookup: null,
	datapackage: null,

	loadDataset(path) {
		this.datapackagePath = this.getDatapackagePath(path);
		this.basePath = this.getBasePath(this.datapackagePath);
		return this.readDataPackage(this.datapackagePath);
	},

	getDatapackagePath(path) {	
		if (!path.endsWith("datapackage.json")) {
			if (!path.endsWith("/")) {
				path = path + "/";
			}
			path = path + "datapackage.json";
		}
		return path;
	},

	getBasePath(datapackagePath) {
		dpPathSplit = datapackagePath.split("/");
		dpPathSplit.pop();
		return dpPathSplit.join("/") + "/";
	},

	readDataPackage(path) {
		return fetch(path)
			.then(response => response.json())
			.then(this.handleNewDatapackage.bind(this));
	},

	handleNewDatapackage(dpJson) {
		this.datapackage = dpJson;
		this.buildResourcesLookup();
		this.buildKeyValueLookup();
		return this.buildConceptsLookup().then(() => dpJson);
	},

	buildConceptsLookup() {
		return this.performQuery({ select: { key: ["concept"], value: ["concept_type", "domain"] }, from: "concepts" }).then(result => {
			result = result
				.filter(concept => concept.concept_type == "entity_set")
				.map(concept => ({ concept: "is--" + concept.concept, concept_type: "boolean", domain: null }))
				.concat(result);
			this.concepts = result;
			this.conceptsLookup = this.buildDataLookup(this.concepts, "concept");
		});
	},

	// can only take single-dimensional data 
	buildDataLookup(data, key) {
		return new Map(data.map(row => [row[key], row]));
	},

	performQuery(query) {
		const { select, from="", where = {}, join = {}, order } = query;

		// schema queries can be answered synchronously (after datapackage is loaded)
		if (from.split(".")[1] == "schema") 
			return Promise.resolve(this.getSchemaResponse(query))

		// other queries are async
		return new Promise((resolve, reject) => {

			const projection = select.key.concat(select.value);
			const filterFields = this.getFilterFields(where).filter(field => !projection.includes(field));
			select.value = select.value.concat(filterFields); // add fields found only in filter to select.value so they are fetched and can be used for filtering

			const resourcesPromise    = this.loadResources(select.key, select.value); // load all relevant resources
			const joinsPromise        = this.getJoinLists(join, query);               // list of values selected from a join clause, effectively used to filter
			const entityFilterPromise = this.getEntityFilter(select.key);             // filter which ensures result only includes queried entity sets

			Promise.all([resourcesPromise, entityFilterPromise, joinsPromise])
				.then(([resources, entityFilter, joinLists]) => {

					this.resolveJoinsInWhere(where, joinLists);            // replace $join placeholders with { $in: [...] } operators
					const filter = this.mergeFilters(entityFilter, where);

					const response = resources
						.map(resourceResponse => this.applyProjectionAndRenameHeader(resourceResponse, select))
						.reduce((joinedData, projected) => this.joinData(select.key, joinedData, projected), []) // join resources to one response
						.map(row => this.fillMissingValues(row, projection)) // fill any missing values with null values
						.filter(row => this.applyFilterRow(row, filter)) // apply filters (entity sets and where (including join))
						.map(row => { filterFields.forEach(field => delete row[field]); return row; }); // remove fields found only in filter. 
						// Cleaner: Do projection after filter and don't include these fields in projection.

					resolve(response);

				});       
		});
	},

	/**
	 * Replaces `$join` placeholders with relevant `{ "$in": [...] }` operator. Impure method: `where` parameter is edited.
	 * @param  {Object} where     Where clause possibly containing $join placeholders as field values. 
	 * @param  {Object} joinLists Collection of lists of entity or time values, coming from other tables defined in query `join` clause.
	 * @return {undefined}        Changes where parameter in-place. Does not return.
	 */
	resolveJoinsInWhere(where, joinLists) {
		for (field in where) {
			// no support for deeper object structures (mongo style) { foo: { bar: "3", baz: true }}
			if (["$and","$or","$nor"].includes(field))
				where[field].forEach(subFilter => this.resolveJoinsInWhere(subFilter, joinLists));
			else if (field == "$not")
				this.resolveJoinsInWhere(where[field], joinLists);
			else if (joinLists[where[field]])
				where[field] = { "$in": joinLists[where[field]] };
		};
	},

	mergeFilters(...filters) {
		return filters.reduce((a,b) => { 
			a["$and"].push(b); 
			return a 
		}, { "$and": [] });
	},

	getSchemaResponse(query) {
		const collection = query.from.split('.')[0];
		const getSchemaFromCollection = collection => this.datapackage.ddfSchema[collection].map(kvPair => ({ key: kvPair.primaryKey, value: kvPair.value }))
		if (this.datapackage.ddfSchema[collection]) {
			return getSchemaFromCollection(collection);
		} else if (collection == "*") {
			return Object.keys(this.datapackage.ddfSchema)
				.map(getSchemaFromCollection)
				.reduce((a,b) => a.concat(b));
		} else {
			this.throwError("No valid collection (" + collection + ") for schema query");
		}
	},

	fillMissingValues(row, projection) {
		for(let field of projection) {
			if (typeof row[field] == "undefined") row[field] = null;
		}
		return row;
	},

	getOperator(op) {
		const ops = {
			/* logical operators */
			$and: (row, predicates) => predicates.map(p => this.applyFilterRow(row,p)).reduce((a,b) => a && b),
			$or:  (row, predicates) => predicates.map(p => this.applyFilterRow(row,p)).reduce((a,b) => a || b),
			$not: (row, predicates) => !this.applyFilterRow(row, predicate, q),
			$nor: (row, predicates) => !predicates.map(p => this.applyFilterRow(row,p)).reduce((a,b) => a || b),

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

	applyFilterRow(row, filter) {
		return Object.keys(filter).reduce((result, filterKey) => {
			if (operator = this.getOperator(filterKey)) {
				// apply operator
				return result && operator(row, filter[filterKey]);
			} else if(typeof filter[filterKey] == "string") { 
				// { <field>: <value> } is shorthand for { <field>: { $eq: <value> }} 
				return result && this.getOperator("$eq")(row[filterKey], filter[filterKey]);
			} else {
				// go one step deeper - doesn't happen yet with DDFQL queries as fields have no depth
				return result && this.applyFilterRow(row[filterKey], filter[filterKey]);
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
			// no support for deeper object structures (mongo style)
			if (["$and","$or","$not","$nor"].includes(field))
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
	getEntityConceptRenameMap(queryKey, resourceKey) {
		const resourceKeySet = new Set(resourceKey);
		return this.filterConceptsByType(queryKey, ["entity_set", "entity_domain"])
			.map(concept => this.concepts
				.filter(lookupConcept => {  
					if (concept.concept_type == "entity_set")
						return resourceKeySet.has(lookupConcept.concept) && 
							lookupConcept.concept != concept.concept && // not the actual concept
							(
								lookupConcept.domain == concept.domain ||  // other entity sets in entity domain
								lookupConcept.concept == concept.domain    // entity domain of the entity set
							) 
					else // concept_type == "entity_domain"
						return resourceKeySet.has(lookupConcept.concept) && 
							lookupConcept.concept != concept.concept && // not the actual concept
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

	loadResources(key, value) {
		const resources = this.getResources(key, value);
		return Promise.all([...resources].map(this.loadResource.bind(this)));
	},

	/**
	 * Applies a projection to data and renames headers according to a entity concept rename map.
	 * @param  {Response object} resourceResponse Response object from resource query
	 * @param  {Select clause object} projection  Object with key and value arrays for respective projections
	 * @return {2d array}                         Data from resourceResponse with projection and renaming applied
	 */
	applyProjectionAndRenameHeader(resourceResponse, projection) {
		const renamed = this.renameEntityHeaders(resourceResponse, projection.key);
		return this.projectData(renamed, projection.key.concat(projection.value));
		/*
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
		*/
	},

	projectData(data, projection) {
		const projectionSet = new Set(projection);
		return data.map(row => {
			const result = {};
			for (concept in row) {
				if (projectionSet.has(concept)) {
					result[concept] = row[concept];
				}
			}
			return result;
		});
	},

	renameHeader(data, renameMap) {
		return data.map(row => {
			const result = {};
			for (concept in row) {
				if (renameMap.has(concept)) 
					result[renameMap.get(concept)] = row[concept];
				else
					result[concept] = row[concept];
			}
			return result;
		});		
	},

	renameEntityHeaders(resourceResponse, keyProjection) {
		const renameMap = this.getEntityConceptRenameMap(keyProjection, resourceResponse.resource.schema.primaryKey);
		return this.renameHeader(resourceResponse.data, renameMap);
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
					// can't do dynamic typing without concept types loaded. concept properties are not parsed
					// TODO: concept handling in two steps: first concept & concept_type, then other properties
					if (!_this.conceptsLookup) return true;
					// parsing to number/boolean based on concept type
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

	buildResourcesLookup() {
		if (this.resourcesLookup) return this.resourcesLookup;
		this.datapackage.resources.forEach(resource => { 
			if (!Array.isArray(resource.schema.primaryKey)) {
				resource.schema.primaryKey = [resource.schema.primaryKey];
			}
		});
		this.resourcesLookup = new Map(this.datapackage.resources.map(resource => [resource.name, resource]));
		return this.resourcesLookup;
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
	}
}