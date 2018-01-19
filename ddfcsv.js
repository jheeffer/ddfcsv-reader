function ddfcsvReader(path) {

	const internalConcepts = [
		{ concept: "concept", concept_type: "string", domain: null },
		{ concept: "concept_type", concept_type: "string", domain: null }
	];

	const operators = new Map([
		/* logical operators */
		["$and", (row, predicates) => predicates.every(p => applyFilterRow(row,p))],
		["$or",  (row, predicates) => predicates.some(p => applyFilterRow(row,p))],
		["$not", (row, predicate) => !applyFilterRow(row, predicate)],
		["$nor", (row, predicates) => !predicates.some(p => applyFilterRow(row,p))],

		/* equality operators */
		["$eq",  (rowValue, filterValue) => rowValue == filterValue],
		["$ne",  (rowValue, filterValue) => rowValue != filterValue],
		["$gt",  (rowValue, filterValue) => rowValue > filterValue],
		["$gte", (rowValue, filterValue) => rowValue >= filterValue],
		["$lt",  (rowValue, filterValue) => rowValue < filterValue],
		["$lte", (rowValue, filterValue) => rowValue <= filterValue],
		["$in",  (rowValue, filterValue) => filterValue.has(rowValue)],
		["$nin", (rowValue, filterValue) => !filterValue.has(rowValue)],
	]);

	const 
		datapackagePath    = getDatapackagePath(path),
		basePath           = getBasePath(datapackagePath),
		datapackagePromise = loadDataPackage(datapackagePath),
		conceptsPromise    = datapackagePromise.then(loadConcepts);

	const
		keyValueLookup  = new Map(),
		resourcesLookup = new Map(),
		conceptsLookup  = new Map();

	let
		datapackage;

	return {
		query
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
		const dpPathSplit = datapackagePath.split("/");
		dpPathSplit.pop();
		return dpPathSplit.join("/") + "/";
	}

	function loadDataPackage(path) {
		return fetch(path)
			.then(response => response.json())
			.then(dp => {
				datapackage = dp;
				buildResourcesLookup(datapackage);
				buildKeyValueLookup(datapackage);
				return datapackage;
			});
	}

	function loadConcepts() {
		// start off with internal concepts
		setConceptsLookup(internalConcepts, "concept");

		// query concepts
		const conceptQuery = {
			select: { key: ["concept"], value: ["concept_type", "domain"] }, 
			from: "concepts" 
		};
		// not using query() to circumvent the conceptPromise resolving
		return queryData(conceptQuery)
			.then(buildConceptsLookup)
			// with conceptsLookup built, we can parse other concept properties
			// according to their concept_type
			.then(reparseConcepts);
	}

	function buildConceptsLookup(concepts) {

		const entitySetMembershipConcepts = concepts
			.filter(concept => concept.concept_type == "entity_set")
			.map(concept => ({ 
				concept: "is--" + concept.concept, 
				concept_type: "boolean", 
				domain: null 
			}));

		concepts = concepts
			.concat(entitySetMembershipConcepts)
			.concat(internalConcepts);

		setConceptsLookup(concepts, "concept");
		return conceptsLookup;
	}

	/**
	 * Iterates resources for query and applies parsing according to concept_type
	 * of headers. Does not take into account join clause.
	 * Impure function as it parses data in-place.
	 * @param  {object} query Query to parse
	 * @return {[type]}       [description]
	 */
	function reparseConcepts(query) {

		const parsingFunctions = new Map([
			["boolean", (string) => string == "true" || string == "TRUE"],
			["measure", (string) => parseFloat(string)]
		]);
		
		const resources = getResources(["concept"]);

		const resourceUpdates = [...resources].map(resource => {
			return resource.data.then(response => {

				// first find out which resource concepts need parsing
				const resourceConcepts = Object.keys(response.data[0]);
				const parsingConcepts = new Map();
				resourceConcepts.forEach(concept => {
					const type = conceptsLookup.get(concept).concept_type;
					if (fn = parsingFunctions.get(type))
						parsingConcepts.set(concept, fn);
				});

				// then parse only those concepts
				return response.data.forEach(row => {
					for([concept, parsefn] of parsingConcepts) {
						row[concept] = parsefn(row[concept]);
					}
				});

			});
		});
		return Promise.all(resourceUpdates);
	}

	// can only take single-dimensional key 
	function setConceptsLookup(concepts) {
		conceptsLookup.clear();
		concepts.forEach(row => conceptsLookup.set(row["concept"], row));
	}

	function query(query) {
		if (isSchemaQuery(query)) {
			return datapackagePromise.then(() => querySchema(query));
		} else {
			return conceptsPromise.then(() => queryData(query)); 
		}
	}

	function isSchemaQuery({ from = "" } = query) { 
		return from.split(".")[1] == "schema";
	}

	function queryData(query) {
		const { 
			select: { key=[], value=[] },
			from  = "", 
			where = {}, 
			join  = {}, 
			order_by = [], 
			language 
		} = query;
		const select = { key, value }

		const projection   = new Set(select.key.concat(select.value));
		const filterFields = getFilterFields(where).filter(field => !projection.has(field));

		const resourcesPromise       = loadResources(select.key, [...select.value, ...filterFields], language); // load all relevant resources
		const joinsPromise           = getJoinFilters(join);    // list of entities selected from a join clause, later insterted in where clause
		const entitySetFilterPromise = getEntitySetFilter(select.key);  // filter which ensures result only includes queried entity sets

		return Promise.all([resourcesPromise, entitySetFilterPromise, joinsPromise])
			.then(([resourceResponses, entitySetFilter, joinFilters]) => {

				// create filter from where, join filters and entity set filters
				const whereResolved = processWhere(where, joinFilters); 
				const filter = mergeFilters(entitySetFilter, whereResolved);

				const dataTables = resourceResponses
					.map(response => processResourceResponse(response, select, filterFields)); // rename key-columns and remove irrelevant value-columns
				
				const queryResult = joinData(select.key, "overwrite", ...dataTables)  // join (reduce) data to one data table
					.filter(row => applyFilterRow(row, filter))     // apply filters (entity sets and where (including join))
					.map(row => fillMissingValues(row, projection)) // fill any missing values with null values
					.map(row => projectRow(row, projection));       // remove fields used only for filtering 

				orderData(queryResult, order_by);

				return queryResult;

			}); 	
	}

	function orderData(data, order_by = []) {
		if (order_by.length == 0) return;

		// process ["geo"] or [{"geo": "asc"}] to [{ concept: "geo", direction: 1 }];
		const orderNormalized = order_by.map(orderPart => {
			if (typeof orderPart == "string") {
				return { concept: orderPart, direction: 1 };
			}	else {
				const concept   = Object.keys(orderPart)[0];
				const direction = (orderPart[concept] == "asc" ? 1 : -1);
				return { concept, direction };
			}
		});

		// sort by one or more fields
		const n = orderNormalized.length;
		data.sort((a,b) => {
			for (var i = 0; i < n; i++) {
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
	 * Replaces $in- and $nin-arrays with sets for faster filtering
	 * @param  {Object} where     Where clause possibly containing $join placeholders as field values. 
	 * @param  {Object} joinFilters Collection of lists of entity or time values, coming from other tables defined in query `join` clause.
	 * @return {Object}           Where clause with $join placeholders replaced by valid filter statements
	 */
	function processWhere(where, joinFilters) {
		const result = {};
		for (field in where) {
			var fieldValue = where[field];
			if (["$and","$or","$nor"].includes(field)) {
				result[field] = fieldValue.map(subFilter => processWhere(subFilter, joinFilters));
			} 
			else if (field == "$in" || field == "$nin") {
				// prepare "$in" fields for optimized lookup
				result[field] = new Set(fieldValue);
			} 
			else if (typeof joinFilters[fieldValue] != "undefined") {
				// found a join!
				// not assigning to result[field] because joinFilter can contain $and/$or statements in case of time concept (join-where is directly copied, not executed)
				// otherwise could end up with where: { year: { $and: [{ ... }]}}, which is invalid (no boolean ops inside field objects)
				// in case of entity join, joinFilters contains correct field
				Object.assign(result, joinFilters[fieldValue]);
			} 
			else if (typeof fieldValue == "object") {
				// catches $not and fields with equality operator-objects
				// { <field>: { "$lt": 1500 }}
				result[field] = processWhere(fieldValue, joinFilters);
			} else {
				// catches rest, being all equality operators except for $in and $nin
				// { "$lt": 1500 }
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

	function querySchema(query) {
		
		const getSchemaFromCollection = collection => {
			return datapackage.ddfSchema[collection].map(
				({ primaryKey, value }) => ({ key: primaryKey, value })
			)
		};

		const collection = query.from.split('.')[0];
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

	function applyFilterRow(row, filter) {
		// implicit $and in filter object handled by .every()
		return Object.keys(filter).every(filterKey => {
			if (operator = operators.get(filterKey)) {
				return operator(row, filter[filterKey]);
			} else if(typeof filter[filterKey] != "object") { // assuming values are primitives not Number/Boolean/String objects
				// { <field>: <value> } is shorthand for { <field>: { $eq: <value> }} 
				return operators.get("$eq")(row[filterKey], filter[filterKey]);
			} else {
				// filter[filterKey] is an object and will thus contain 
				// an equality operator (no deep objects (like in Mongo) supported)
				return applyFilterRow(row[filterKey], filter[filterKey]);
			}
		});
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
			return query({ select: { key: [join.key] }, where: join.where })
				.then(result => ({ 
					[joinID]: {
						[join.key]: { 
							"$in": new Set(result.map(row => row[join.key])) 
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
	function filterConceptsByType(concept_types, conceptStrings = conceptsLookup.keys()) {
		const concepts = [];
		for (conceptString of conceptStrings) {
			const concept = conceptsLookup.get(conceptString);
			if (concept_types.includes(concept.concept_type)) {
				concepts.push(concept);
			}
		};
		return concepts;
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
	function getEntitySetFilter(conceptStrings) {
		const promises = filterConceptsByType(["entity_set"], conceptStrings)
			.map(concept => query({ select: { key: [concept.domain], value: ["is--" + concept.concept] } })
				.then(result => ({ [concept.concept]:
						{ "$in": new Set(
								result
									.filter(row => row["is--" + concept.concept])
									.map(row => row[concept.domain])
							)
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
		const resources = getResources(key, value);
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
					if (resultRow[concept] !== undefined && resultRow[concept] !== sourceRow[concept]) {
						throwError("JOIN Error: two resources have different data for `" + concept + "`: " + JSON.stringify(sourceRow) + "," + JSON.stringify(resultRow) + ".");
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

	function buildResourcesLookup(datapackage) {
		if (resourcesLookup.size > 0) return resourcesLookup;
		datapackage.resources.forEach(resource => { 
			if (!Array.isArray(resource.schema.primaryKey)) {
				resource.schema.primaryKey = [resource.schema.primaryKey];
			}
			resource.translations = {};
			resourcesLookup.set(resource.name, resource);
		});
		return resourcesLookup;
	}

	function buildKeyValueLookup(datapackage) {
		if (keyValueLookup.size > 0) return keyValueLookup;
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
