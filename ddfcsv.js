function ddfcsvReader(path) {

	let 
		keyValueLookup,
		resourcesLookup,
		conceptsLookup,
		datapackagePath,
		basePath,
		datapackage;

	return loadDataset(path).then(() => ({
		performQuery
	}));

	function loadDataset(path) {
		datapackagePath = getDatapackagePath(path);
		basePath = getBasePath(datapackagePath);
		return readDataPackage(datapackagePath);
	}

	function getDatapackagePath(path) {	
		if (!path.endsWith("datapackage.json")) {
			if (!path.endsWith("/")) {
				path = path + "/";
			}
			path = path + "datapackage.json";
		}
		return path;
	}

	function getBasePath(datapackagePath) {
		dpPathSplit = datapackagePath.split("/");
		dpPathSplit.pop();
		return dpPathSplit.join("/") + "/";
	}

	function readDataPackage(path) {
		return fetch(path)
			.then(response => response.json())
			.then(handleNewDatapackage);
	}

	function handleNewDatapackage(dpJson) {
		datapackage = dpJson;
		buildResourcesLookup();
		buildKeyValueLookup();
		return buildConceptsLookup().then(() => dpJson);
	}

	function buildConceptsLookup() {

		// start off with internal concepts
		const internalConcepts = [
			{ concept: "concept", concept_type: "string", domain: null },
			{ concept: "concept_type", concept_type: "string", domain: null }
		];
		conceptsLookup = buildDataLookup(internalConcepts, "concept");

		// query concepts
		const conceptQuery = {
			select: { key: ["concept"], value: ["concept_type", "domain"] }, 
			from: "concepts" 
		};
		return performQuery(conceptQuery).then(result => {
			result = result
				.filter(concept => concept.concept_type == "entity_set")
				.map(concept => ({ 
					concept: "is--" + concept.concept, 
					concept_type: "boolean", 
					domain: null 
				}))
				.concat(result)
				.concat(internalConcepts);
			conceptsLookup = buildDataLookup(result, "concept");

			// with conceptsLookup built, we can parse other concept properties
			// according to their concept_type
			return reparseResources(conceptQuery);
		});
	}

	/**
	 * Goes over resources for query and applies parsing according to concept_type
	 * of headers. Impure function as it changes resources' data.
	 * @param  {object} query Query to parse
	 * @return {[type]}       [description]
	 */
	function reparseResources(query) {
		const resources = getResources(query.select.key, query.select.value);
		const resourceUpdates = [...resources].map(resource => {
			return resource.data.then(response => response.data.forEach(row => {
				for(field of Object.keys(row)) {
					const type = conceptsLookup.get(field).concept_type;
					if (type == "boolean")
						row[field] = row[field] == "true" || row[field] == "TRUE";
					if (type == "measure")
						row[field] = parseFloat(row[field]);
				}
			}));
		});
		return Promise.all(resourceUpdates);
	}

	// can only take single-dimensional data 
	function buildDataLookup(data, key) {
		return new Map(data.map(row => [row[key], row]));
	}

	function performQuery(query) {
		//console.log("Incoming query: ", query);
		const { 
			select: { key=[], value=[] },
			from  = "", 
			where = {}, 
			join  = {}, 
			order_by = [], 
			language 
		} = query;
		const select = { key, value }

		// schema queries can be answered synchronously (after datapackage is loaded)
		if (from.split(".")[1] == "schema") 
			return Promise.resolve(getSchemaResponse(query))

		// other queries are async
		return new Promise((resolve, reject) => {

			const projection = new Set(select.key.concat(select.value));
			const filterFields = getFilterFields(where).filter(field => !projection.has(field));

			const resourcesPromise    = loadResources(select.key, [...select.value, ...filterFields], language); // load all relevant resources
			const joinsPromise        = getJoinFilters(join, query);    // list of entities selected from a join clause, later insterted in where clause
			const entityFilterPromise = getEntityFilter(select.key);  // filter which ensures result only includes queried entity sets

			Promise.all([resourcesPromise, entityFilterPromise, joinsPromise])
				.then(([resourceResponses, entityFilter, joinFilters]) => {

					const whereResolved = resolveJoinsInWhere(where, joinFilters); // replace $join placeholders with { $in: [...] } operators
					const filter = mergeFilters(entityFilter, whereResolved);

					const dataTables = resourceResponses
						.map(response => processResourceResponse(response, select, filterFields)); // rename key-columns and remove irrelevant value-columns
					
					const queryResult = joinData(select.key, "overwrite", ...dataTables)  // join (reduce) data to one data table
						.filter(row => applyFilterRow(row, filter))     // apply filters (entity sets and where (including join))
						.map(row => fillMissingValues(row, projection)) // fill any missing values with null values
						.map(row => projectRow(row, projection));       // remove fields used only for filtering 

					orderData(queryResult, order_by);

					resolve(queryResult);

				});       
		});
	}

	function orderData(data, order_by = []) {
		if (order_by.length == 0) return;

		// process ["geo"] or [{"geo": "asc"}] to [{ concept: "geo", order: 1 }];
		const orderNormalized = order_by.map(orderPart => {
			if (typeof orderPart == "string") {
				return { concept: orderPart, order: 1 };
			}	else {
				const concept   = Object.keys(orderPart)[0];
				const direction = (orderPart[concept] == "asc" ? 1 : -1);
				return { concept, direction };
			}
		});

		// sort by one or more fields
		const n = orderNormalized.length;
		data.sort((a,b) => {
			for (let i = 0; i < n; i++) {
				const order = orderNormalized[i];
				if (a[order.concept] < b[order.concept])
					return -1 * order.direction;
				else if (a[order.concept] > b[order.concept])
					return 1 * order.direction;
			} 
			return 0;
		});

	}

	/**
	 * Replaces `$join` placeholders with relevant `{ "$in": [...] }` operator.
	 * @param  {Object} where     Where clause possibly containing $join placeholders as field values. 
	 * @param  {Object} joinFilters Collection of lists of entity or time values, coming from other tables defined in query `join` clause.
	 * @return {Object}           Where clause with $join placeholders replaced by valid filter statements
	 */
	function resolveJoinsInWhere(where, joinFilters) {
		const result = {};
		for (field in where) {
			var fieldValue = where[field];
			// no support for deeper object structures (mongo style) { foo: { bar: "3", baz: true }}
			if (["$and","$or","$nor"].includes(field))
				result[field] = fieldValue.map(subFilter => resolveJoinsInWhere(subFilter, joinFilters));
			else if (field == "$not")
				result[field] = resolveJoinsInWhere(fieldValue, joinFilters);
			else if (typeof joinFilters[fieldValue] != "undefined") {
				// not assigning to result[field] because joinFilter can contain $and/$or statements in case of time concept (join-where is directly copied, not executed)
				// otherwise could end up with where: { year: { $and: [{ ... }]}}, which is invalid (no boolean ops inside field objects)
				// in case of entity join, joinFilters contains correct field
				Object.assign(result, joinFilters[fieldValue]);
			} else {
				result[field] = fieldValue;
			}
		};
		return result;
	}

	function mergeFilters(...filters) {
		return filters.reduce((a,b) => { 
			a["$and"].push(b); 
			return a 
		}, { "$and": [] });
	}

	function getSchemaResponse(query) {
		const collection = query.from.split('.')[0];
		const getSchemaFromCollection = collection => datapackage.ddfSchema[collection].map(kvPair => ({ key: kvPair.primaryKey, value: kvPair.value }))
		if (datapackage.ddfSchema[collection]) {
			return getSchemaFromCollection(collection);
		} else if (collection == "*") {
			return Object.keys(datapackage.ddfSchema)
				.map(getSchemaFromCollection)
				.reduce((a,b) => a.concat(b));
		} else {
			throwError("No valid collection (" + collection + ") for schema query");
		}
	}

	function fillMissingValues(row, projection) {
		for(let field of projection) {
			if (typeof row[field] == "undefined") row[field] = null;
		}
		return row;
	}

	function getOperator(op) {
		const ops = {
			/* logical operators */
			$and: (row, predicates) => predicates.map(p => applyFilterRow(row,p)).reduce((a,b) => a && b),
			$or:  (row, predicates) => predicates.map(p => applyFilterRow(row,p)).reduce((a,b) => a || b),
			$not: (row, predicates) => !applyFilterRow(row, predicate),
			$nor: (row, predicates) => !predicates.map(p => applyFilterRow(row,p)).reduce((a,b) => a || b),

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
	}

	function applyFilterRow(row, filter) {
		return Object.keys(filter).reduce((result, filterKey) => {
			if (operator = getOperator(filterKey)) {
				// apply operator
				return result && operator(row, filter[filterKey]);
			} else if(typeof filter[filterKey] != "object") { // assuming values are primitives not Number/Boolean/String objects
				// { <field>: <value> } is shorthand for { <field>: { $eq: <value> }} 
				return result && getOperator("$eq")(row[filterKey], filter[filterKey]);
			} else {
				// go one step deeper - doesn't happen yet with DDFQL queries as fields have no depth
				return result && applyFilterRow(row[filterKey], filter[filterKey]);
			}
		}, true);
	}

	function getJoinFilters(join) {
		return Promise.all(Object.keys(join).map(joinID => getJoinFilter(joinID, join[joinID])))
			.then(results => results.reduce(mergeObjects, {}));
	}

	function mergeObjects(a,b) { return Object.assign(a,b) };

	function getJoinFilter(joinID, join) {
		// assumption: join.key is same as field in where clause 
		//  - where: { geo: $geo }, join: { "$geo": { key: geo, where: { ... }}}
		//  - where: { year: $year }, join: { "$year": { key: year, where { ... }}}
		if (conceptsLookup.get(join.key).concept_type == "time") {
			// time, no query needed as time values are not explicit in the dataset
			// assumption: there are no time-properties. E.g. data like <year>,population
			return Promise.resolve({ [joinID]: join.where });
		}	else {
			// entity concept
			return performQuery({ select: { key: [join.key] }, where: join.where })
				.then(result => ({ 
					[joinID]: {
						[join.key]: { 
							"$in": result.map(row => row[join.key]) 
						} 
					}
				}));
		}
	}

	function getFilterFields(filter) {
		const fields = [];
		for (field in filter) {
			// no support for deeper object structures (mongo style)
			if (["$and","$or","$not","$nor"].includes(field))
				filter[field].map(getFilterFields).forEach(subFields => fields.push(...subFields))
			else
				fields.push(field);
		};
		return fields;
	}

	/**
	 * Filter concepts by type
	 * @param  {Array} conceptStrings   Array of concept strings to filter out. Default all concepts.
	 * @param  {Array} concept_types    Array of concept types to filter out
	 * @return {Array}                  Array of concept strings only of given types
	 */
	function filterConceptsByType(concept_types, conceptStrings = [...conceptsLookup.keys()]) {
		return conceptStrings
			.filter(conceptString => conceptsLookup && concept_types.includes(conceptsLookup.get(conceptString).concept_type))
			.map(conceptString => conceptsLookup.get(conceptString));
	}

	/**
	 * Find the aliases an entity concept can have
	 * @param  {Array} conceptStrings An array of concept strings for which entity aliases are found if they're entity concepts
	 * @return {Map}                  Map with all aliases as keys and the entity concept as value
	 */
	function getEntityConceptRenameMap(queryKey, resourceKey) {
		const resourceKeySet = new Set(resourceKey);
		const entityConceptTypes = ["entity_set", "entity_domain"];
		const queryEntityConcepts = filterConceptsByType(entityConceptTypes, queryKey);
		if (queryEntityConcepts.length == 0) return new Map();
		
		const allEntityConcepts = filterConceptsByType(entityConceptTypes);

		return queryEntityConcepts
			.map(concept => allEntityConcepts
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
	}

	/**
	 * Get a "$in" filter containing all entities for a entity concept.
	 * @param  {Array} conceptStrings Array of concept strings for which entities should be found
	 * @return {Array}                Array of filter objects for each entity concept
	 */
	function getEntityFilter(conceptStrings) {
		const promises = filterConceptsByType(["entity_set"], conceptStrings)
			.map(concept => performQuery({ select: { key: [concept.domain], value: ["is--" + concept.concept] } })
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
	}

	/**
	 * Returns all resources for a certain key value pair or multiple values for one key
	 * @param  {Array} key          The key of the requested resources
	 * @param  {Array/string} value The value or values found in the requested resources
	 * @return {Array}              Array of resource objects
	 */
	function getResources(key, value) {
		// value not given, load all resources for key
		if (!value || value.length == 0) {
			return new Set(
				[...keyValueLookup
					.get(createKeyString(key))
					.values()
				].reduce((a,b) => a.concat(b))
			)
		}
		// multiple values
		if (Array.isArray(value)) {
			return value
				.map(singleValue => getResources(key,singleValue))
				.reduce((resultSet,resources) => new Set([...resultSet,...resources]), new Set());
		}
		// one key, one value
		return new Set(
			keyValueLookup
				.get(createKeyString(key))
				.get(value)
		);
	}

	function processResourceResponse(response, select, filterFields) {
		const resourcePK = response.resource.schema.primaryKey;
		const resourceProjection = new Set([...resourcePK, ...select.value, ...filterFields]); // all fields used for select or filters
		const renameMap = getEntityConceptRenameMap(select.key, resourcePK);     // rename map to rename relevant entity headers to requested entity concepts

		// Renaming must happen after projection to prevent ambiguity 
		// E.g. a resource with `<geo>,name,region` fields. 
		// Assume `region` is an entity set in domain `geo`.
		// { select: { key: ["region"], value: ["name"] } } is queried
		// If one did rename first the file would have headers `<region>,name,region`. 
		// This would be invalid and make unambiguous projection impossible.
		// Thus we need to apply projection first with result: `<geo>,name`, then we can rename.
		return response.data
			.map(row => projectRow(row, resourceProjection))	// remove fields not used for select or filter
			.map(row => renameHeaderRow(row, renameMap));    // rename header rows (must happen **after** projection)
	}

	function loadResources(key, value, language) {
		const resources = getResources(key, value, language);
		return Promise.all([...resources].map(
			resource => loadResource(resource, language)
		));
	}

	function projectRow(row, projectionSet) {
		const result = {};
		for (concept in row) {
			if (projectionSet.has(concept)) {
				result[concept] = row[concept];
			}
		}
		return result;	
	}

	function renameHeaderRow(row, renameMap) {
		const result = {};
		for (concept in row) {
			result[renameMap.get(concept) || concept] = row[concept];
		}
		return result;		
	}

	function joinData(key, joinMode, ...data) {
		if (data.length == 1) return data[0];
		const canonicalKey = key.slice(0).sort();
		const dataMap = data.reduce((result, data) => {
			data.forEach(row => {
				const keyString = canonicalKey
					.map(concept => row[concept])
					.join(",");
				if (result.has(keyString)) {
					const resultRow = result.get(keyString);
					joinRow(resultRow, row, joinMode);
				} else {
					result.set(keyString, row)
				}
			})
			return result;
		}, new Map());
		return [...dataMap.values()];
	}

	function joinRow(resultRow, sourceRow, mode) {
		switch(mode) {
			case "overwrite":		
				/* Simple alternative without empty value or error handling */
				Object.assign(resultRow, sourceRow);
				break;
			case "translation":
				// Translation joining ignores empty values 
				// and allows different values for strings (= translations)
				for (concept in sourceRow) {
					if (sourceRow[concept] != "") {
						resultRow[concept] = sourceRow[concept];
					}
				}
				break;
			case "overwriteWithError":
				/* Alternative for "overwrite" with JOIN error detection */
				for (concept in sourceRow) {
					if (resultRow[concept] && resultRow[concept] != sourceRow[concept]) {
						throwError("JOIN Error: two resources have different data for key-value pair " + key + "," + concept + ".");
					} else {
						resultRow[concept] = sourceRow[concept];
					}
				}		
				break;				
		}
	}

	function throwError(error) {
		console.error(error);
	}

	function createKeyString(key, row = false) {
		const canonicalKey = key.slice(0).sort();
		if (!row)
			return canonicalKey.join(",");
		else
			return canonicalKey
				.map(concept => row[concept])
				.join(",");
	}

	function loadResource(resource, language) {
		const filePromises = [];

		if (typeof resource.data == "undefined") {
			resource.data = loadFile(basePath + resource.path);
		}
		filePromises.push(resource.data);

		const languageValid = typeof language != "undefined" && getLanguages().includes(language);
		const languageLoaded = typeof resource.translations[language] != "undefined";
		if (languageValid) {
			if (!languageLoaded) {
				const path = basePath + "lang/" + language + "/" + resource.path;
				resource.translations[language] = loadFile(path);	
			}
			filePromises.push(resource.translations[language]);
		}	

		return Promise.all(filePromises).then(fileResponses => {
			const filesData = fileResponses.map(resp => resp.data);
			const primaryKey = resource.schema.primaryKey;
			const data = joinData(primaryKey, "translation", ...filesData);
			return { data, resource };
		});

	}

	function getLanguages() {
		return [
			datapackage.translations.map(lang => lang.id)
		];
	}

	function loadFile(filePath) {
		return new Promise((resolve, reject) => {
			Papa.parse(filePath, {
				download: true,
				header: true,
				skipEmptyLines: true,
				dynamicTyping: (headerName) => {
					// can't do dynamic typing without concept types loaded. 
					// concept properties are not parsed in first concept query
					// reparsing of concepts resource is done in conceptLookup building
					if (!conceptsLookup) return true;

					// parsing to number/boolean based on concept type
					const concept = conceptsLookup.get(headerName) || {};
					return ["boolean", "measure"].includes(concept.concept_type);
				},
				complete: result => resolve(result),
				error: error => reject(error)
			})
		});
	}

	function buildResourcesLookup() {
		if (resourcesLookup) return resourcesLookup;
		datapackage.resources.forEach(resource => { 
			if (!Array.isArray(resource.schema.primaryKey)) {
				resource.schema.primaryKey = [resource.schema.primaryKey];
			}
			resource.translations = {};
		});
		resourcesLookup = new Map(datapackage.resources.map(
			resource => [resource.name, resource]
		));
		return resourcesLookup;
	}

	function buildKeyValueLookup() {
		if (keyValueLookup) return keyValueLookup;
		keyValueLookup = new Map();
		for (let collection in datapackage.ddfSchema) {
			datapackage.ddfSchema[collection].map(kvPair => {
				const key = createKeyString(kvPair.primaryKey);
				const resources = kvPair.resources.map(
					resourceName => resourcesLookup.get(resourceName)
				);
				if (keyValueLookup.has(key)) {
					keyValueLookup.get(key).set(kvPair.value, resources);
				} else {
					keyValueLookup.set(key, new Map([[kvPair.value, resources]]));
				}
			})
		};
		return keyValueLookup;
	}
}
