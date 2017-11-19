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
		return new Promise((resolve, reject) => {
			const { select, from, where, join, order } = query;
			const entitySetValues = this.getEntitySetValues(select.key);
			const resources = this.getResources(select.key, select.value);
			Promise.all([...resources].map(this.loadResource.bind(this)))
				.then(responses => {
					let result = responses.reduce((joinedData, resourceResponse) => {
						const projected = this.getProjection(resourceResponse, select, entitySetValues.conceptRenameMap);
						return this.joinData(select.key, joinedData, projected);
					}, []);
					this.fillMissingValues(result, select);
					entitySetValues.valuesPromise.then(entitySetValues => {
						result = this.applyFilter(result, entitySetValues);
						result = this.applyFilter(result, where);
						resolve(result);
					});
				})
		});
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
			$and: (row, predicates) => predicates.map(p => this.applyFilterRow(row,p)).reduce((a,b) => a && b),
			$or:  (row, predicates) => predicates.map(p => this.applyFilterRow(row,p)).reduce((a,b) => a || b),
			$not: (row, predicates) => !this.applyFilterRow(row, predicate),
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

	applyFilter(data, filter = {}) {
		return data.filter((row) => this.applyFilterRow(row, filter));
	},

	applyFilterRow(row, filter) {
		return Object.keys(filter).reduce((result, filterKey) => {
			if (operator = this.getOperator(filterKey)) {
				return result && operator(row, filter[filterKey]);
			} else if(typeof filter[filterKey] == "string") { // { <field>: <value> } is shorthand for { <field>: { $eq: <value> }} 
				return result && this.getOperator("$eq")(row[filterKey], filter[filterKey])
			} else {
				return result && this.applyFilterRow(row[filterKey], filter[filterKey]);
			}
		}, true);
	},

	getEntitySetValues(conceptStrings) {
		const concepts = conceptStrings
			.filter(conceptString => conceptString != "concept" && ["entity_set", "entity_domain"].includes(this.conceptsLookup.get(conceptString).concept_type))
			.map(conceptString => this.conceptsLookup.get(conceptString));
		
		const promises = concepts.map(concept => {
				if (concept.concept_type == "entity_set")
					return this.performQuery({ select: { key: [concept.domain], value: ["is--" + concept.concept] } })
						.then(result => {
							return { [concept.concept]: 
								{ "$in": result
									.filter(row => row["is--" + concept.concept])
									.map(row => row[concept.domain])
								}
							};
						});
				else // concept_type == "entity_domain"
					return Promise.resolve();			
		});
		return {
			conceptRenameMap: concepts
				.map(concept => this.concepts
					.filter(lookupConcept => {	
						if (concept.concept_type == "entity_set")
							return lookupConcept.concept != concept.concept && // the actual concept
								(
									lookupConcept.domain == concept.domain ||  // other entity sets in same domain
									lookupConcept.concept == concept.domain // entity domain of entity set
								) 
						else // concept_type == "entity_domain"
							return lookupConcept.concept != concept.concept && // the actual concept
								lookupConcept.domain == concept.concept // entity sets of the domain
					})
					.reduce((map, aliasConcept) => map.set(aliasConcept.concept, concept.concept), new Map())
				).reduce((mergeA, mergeB) => new Map([...mergeA,...mergeB]), new Map()),
			valuesPromise: Promise.all(promises).then(results => {
				return results.reduce((a,b) => Object.assign(a,b),{});
			})
		}
	},

	getResources(key, value) {
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

	getProjection(resourceResponse, projection, renameMap) {
		projection = { key: new Set(projection.key), value: new Set(projection.value) };
		const pk = new Set(resourceResponse.resource.schema.primaryKey);
		return resourceResponse.data.map(row => {
			const result = {};
			for (concept in row) {
				const oldConcept = concept;
				// concept is in key: renaming can happen
				if (pk.has(concept)) {
					if (renameMap.has(concept)) {
						concept = renameMap.get(concept);
					}
					result[concept] = row[oldConcept];
				}
				// concept is in value: will not interfere with renamed keys
				if (projection.value.has(concept)) {
					result[concept] = row[oldConcept];
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