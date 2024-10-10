ontology_examples = {
    "BIBO": [
        {
            "@context": {
                "bibo": "http://purl.org/ontology/bibo/",
                "dc": "http://purl.org/dc/elements/1.1/",
                "dcterms": "http://purl.org/dc/terms/",
                "foaf": "http://xmlns.com/foaf/0.1/",
                "xsd": "http://www.w3.org/2001/XMLSchema#",
            },
            "@id": "http://example.org/books/1",
            "@type": "bibo:Book",
            "dc:title": "The Lord of the Rings",
            "dc:creator": {"@id": "http://example.org/persons/1"},
            "dcterms:issued": {"@type": "xsd:date", "@value": "1954-07-29"},
            "bibo:isbn": "978-0-261-10236-9",
            "dc:publisher": "George Allen & Unwin",
            "dc:language": "en",
            "bibo:edition": "1st",
            "bibo:series": {
                "@type": "bibo:Series",
                "dc:title": "The Lord of the Rings",
            },
            "bibo:DocumentPart": [
                {
                    "@type": "bibo:DocumentPart",
                    "dcterms:title": "Part 1: The Fellowship of the Ring",
                    "bibo:content": "The first part of the epic novel...",
                },
                {
                    "@type": "bibo:DocumentPart",
                    "dcterms:title": "Part 2: The Two Towers",
                    "bibo:content": "The second part of the epic novel...",
                },
                {
                    "@type": "bibo:DocumentPart",
                    "dcterms:title": "Part 3: The Return of the King",
                    "bibo:content": "The third part of the epic novel...",
                },
            ],
        },
        {
            "@context": {
                "foaf": "http://xmlns.com/foaf/0.1/",
            },
            "@id": "http://example.org/persons/1",
            "@type": "foaf:Person",
            "foaf:name": "J.R.R. Tolkien",
        },
    ],
    "FOAF": [
        {
            "@context": {
                "foaf": "http://xmlns.com/foaf/0.1/",
                "xsd": "http://www.w3.org/2001/XMLSchema#",
            },
            "@id": "http://example.org/persons/alice123",
            "@type": "foaf:Person",
            "foaf:name": "Alice Wonderland",
            "foaf:nick": "alice123",
            "foaf:mbox": "mailto:alice@example.com",
            "foaf:homepage": "http://www.alicewonderland.com",
            "foaf:workplaceHomepage": "http://www.examplecompany.com",
            "foaf:knows": [
                {"@id": "http://example.org/persons/bob456"},
                {"@id": "http://example.org/persons/carol789"},
            ],
            "foaf:member": {"@id": "http://example.org/organizations/examplecompany"},
        },
        {
            "@context": {
                "foaf": "http://xmlns.com/foaf/0.1/",
                "xsd": "http://www.w3.org/2001/XMLSchema#",
            },
            "@id": "http://example.org/persons/bob456",
            "@type": "foaf:Person",
            "foaf:name": "Bob Johnson",
            "foaf:nick": "bob456",
            "foaf:mbox": "mailto:bob@example.com",
            "foaf:homepage": "http://www.bobjohnson.com",
            "foaf:workplaceHomepage": "http://www.anothercompany.com",
        },
        {
            "@context": {
                "foaf": "http://xmlns.com/foaf/0.1/",
                "xsd": "http://www.w3.org/2001/XMLSchema#",
            },
            "@id": "http://example.org/persons/carol789",
            "@type": "foaf:Person",
            "foaf:name": "Carol Smith",
            "foaf:nick": "carol789",
            "foaf:mbox": "mailto:carol@example.com",
            "foaf:homepage": "http://www.carolsmith.com",
            "foaf:workplaceHomepage": "http://www.differentcompany.com",
        },
        {
            "@context": {
                "foaf": "http://xmlns.com/foaf/0.1/",
                "xsd": "http://www.w3.org/2001/XMLSchema#",
            },
            "@id": "http://example.org/organizations/examplecompany",
            "@type": "foaf:Organization",
            "foaf:name": "Example Company",
            "foaf:homepage": "http://www.examplecompany.com",
        },
    ],
    "entertainment": [
        {
            "@context": {
                "schema": "http://schema.org/",
                "xsd": "http://www.w3.org/2001/XMLSchema#",
            },
            "@id": "http://example.org/videogames/1",
            "@type": "schema:VideoGame",
            "schema:name": "World of Warcraft",
            "schema:author": {"@id": "http://example.org/organizations/1"},
            "schema:datePublished": {"@type": "xsd:date", "@value": "2004-11-23"},
            "schema:genre": ["MMORPG", "Fantasy", "Adventure"],
            "schema:gamePlatform": ["PC", "Mac"],
            "schema:description": "World of Warcraft is a massively multiplayer online role-playing game (MMORPG) set in the fantasy world of Azeroth.",
            "schema:character": [
                {"@id": "http://example.org/persons/thrall"},
                {"@id": "http://example.org/persons/jaina"},
                {"@id": "http://example.org/persons/sylvanas"},
            ],
        },
        {
            "@context": {"schema": "http://schema.org/"},
            "@id": "http://example.org/organizations/1",
            "@type": "schema:Organization",
            "schema:name": "Blizzard Entertainment",
            "schema:url": "https://www.blizzard.com/",
        },
        {
            "@context": {"schema": "http://schema.org/"},
            "@id": "http://example.org/persons/thrall",
            "@type": "schema:Person",
            "schema:name": "Thrall",
            "schema:roleName": "Warchief",
            "schema:description": "Thrall is the former Warchief of the Horde and one of the most powerful shamans in Azeroth.",
        },
        {
            "@context": {"schema": "http://schema.org/"},
            "@id": "http://example.org/persons/jaina",
            "@type": "schema:Person",
            "schema:name": "Jaina Proudmoore",
            "schema:roleName": "Archmage",
            "schema:description": "Jaina Proudmoore is a powerful sorceress and the leader of the Kirin Tor.",
        },
        {
            "@context": {"schema": "http://schema.org/"},
            "@id": "http://example.org/persons/sylvanas",
            "@type": "schema:Person",
            "schema:name": "Sylvanas Windrunner",
            "schema:roleName": "Banshee Queen",
            "schema:description": "Sylvanas Windrunner is the former Ranger-General of Silvermoon and the current Warchief of the Horde.",
        },
    ],
    "GS1": [
        {
            "@context": {"gs1": "https://www.gs1.org/voc/"},
            "@type": "gs1:Product",
            "@id": "urn:epc:class:lgtin:product1",
            "gs1:gtin": "0614141107346",
            "gs1:additionalProductClassification": {
                "@type": "gs1:AdditionalProductClassificationDetails",
                "gs1:additionalProductClassificationValue": "Sample Classification",
            },
            "gs1:additionalProductDescription": {
                "@value": "Additional description for the demo product.",
                "@language": "en",
            },
            "gs1:bestBeforeDate": "2024-12-31",
            "gs1:brand": "Demo Brand",
            "gs1:brandOwner": {
                "@type": "gs1:Organization",
                "gs1:name": "Demo Brand Owner",
                "gs1:identifier": "1234567890123",
            },
            "gs1:consumerStorageInstructions": {
                "@value": "Store in a cool, dry place.",
                "@language": "en",
            },
            "gs1:consumerUsageInstructions": {
                "@value": "Use as directed.",
                "@language": "en",
            },
            "gs1:countryOfOrigin": {"@type": "gs1:Country", "gs1:name": "Demo Country"},
            "gs1:descriptiveSize": {"@value": "Medium", "@language": "en"},
            "gs1:expirationDate": "2024-12-31",
            "gs1:functionalName": {"@value": "Demo Functional Name", "@language": "en"},
            "gs1:hasBatchLotNumber": "BATCH1234",
            "gs1:image": {
                "@type": "gs1:ReferencedFileDetails",
                "gs1:url": "http://example.com/demo-product.jpg",
            },
            "gs1:manufacturer": {
                "@type": "gs1:Organization",
                "gs1:name": "Demo Manufacturer",
                "gs1:identifier": "1234567890123",
                "gs1:address": "123 Demo St, Demo City, Demo Country",
                "gs1:contactPoint": "contact@demomanufacturer.com",
            },
            "gs1:netContent": {
                "@type": "gs1:QuantitativeValue",
                "gs1:value": "1",
                "gs1:unitCode": "KGM",
            },
            "gs1:netWeight": {
                "@type": "gs1:QuantitativeValue",
                "gs1:value": "1",
                "gs1:unitCode": "KGM",
            },
            "gs1:productDescription": {
                "@value": "A demo product for testing purposes.",
                "@language": "en",
            },
            "gs1:productName": {"@value": "Demo Product", "@language": "en"},
            "gs1:productionDate": "2023-12-31",
        },
        {
            "@context": {
                "epcis": "https://ref.gs1.org/standards/epcis/2.0.0/epcis-context.jsonld"
            },
            "@id": "https://id.example.org/document1",
            "@type": "epcis:EPCISDocument",
            "epcis:schemaVersion": "2.0",
            "epcis:epcisBody": {
                "epcis:eventList": [
                    {
                        "epcis:eventID": "urn:uuid:b3cf9863-a8f5-4fbc-b794-566d498e1f9c",
                        "epcis:type": "epcis:ObjectEvent",
                        "epcis:action": "epcis:ADD",
                        "epcis:bizStep": "epcis:commissioning",
                        "epcis:disposition": "epcis:active",
                        "epcis:epcList": [{"@id": "urn:epc:class:lgtin:product1"}],
                        "epcis:eventTime": "2023-10-30T11:30:47.000Z",
                        "epcis:bizLocation": {"@id": "urn:gov-si:gmid:100478662"},
                    }
                ]
            },
        },
        {
            "@context": {
                "epcis": "https://ref.gs1.org/standards/epcis/2.0.0/epcis-context.jsonld"
            },
            "@id": "https://id.example.org/document2",
            "@type": "epcis:EPCISDocument",
            "epcis:schemaVersion": "2.0",
            "epcis:epcisBody": {
                "epcis:eventList": [
                    {
                        "epcis:eventID": "urn:uuid:ed785a6a-2231-11ee-be56-0242ac120002",
                        "epcis:type": "epcis:TransformationEvent",
                        "epcis:eventTime": "2023-09-30T20:33:31.116000-06:00",
                        "epcis:inputEPCList": [{"@id": "urn:gov-si:eartag:100507294"}],
                        "epcis:outputEPCList": [
                            {"@id": "urn:epc:class:lgtin:product1"}
                        ],
                        "epcis:bizStep": "epcis:creating_class_instance",
                        "epcis:disposition": "epcis:in_progress",
                        "epcis:bizLocation": {"@id": "urn:gov-si:gmid:100507294"},
                    }
                ]
            },
        },
    ],
    "Brick": [
        {
            "@context": {
                "bc": "https://brickschema.org/schema/Brick#",
                "schema": "http://schema.org/",
            },
            "@id": "urn:facade_2",
            "@type": "bc:Outside",
            "schema:name": "Hospital Real Facade - building diagnosis and material study",
            "bc:isAssociatedWith": {"@id": "urn:building_1"},
            "schema:description": "This is a building diagnosis and material study for Hospital Real, an ancient hospital in Granada built in 1504 following a design by Enrique Egas (late Gothic), although Charles V finished the project in Renaissance style. It is currently the main headquarters of the University of Granada, home to the Rectorate and other central university services. A more detailed description of the building is here: https://patrimonio.ugr.es/bien-inmueble/610/ To my best knowledge, the building has a permanent structural health monitoring system installed. Also, in November 2023, it was fully surveyed by researchers from University of Granada, and the natural vibration modes of the building were identified.",
            "schema:author": {
                "@type": "schema:Person",
                "schema:name": "Javier Gallego Roca",
            },
            "schema:image": [
                {
                    "@type": "schema:ImageObject",
                    "schema:url": "https://res.cloudinary.com/dhupiskro/image/upload/fl_preserve_transparency/v1717661968/facade1_cfau5o.jpg?_s=public-apps",
                }
            ],
            "schema:material": [
                {
                    "@type": "schema:Material",
                    "schema:name": "Travertine",
                    "schema:description": "Material used on the main side",
                },
                {
                    "@type": "schema:Material",
                    "schema:name": "White Macael marble",
                    "schema:description": "Material used on the main side",
                },
            ],
        },
        {
            "@context": {
                "bc": "https://brickschema.org/schema/Brick#",
                "ref": "https://brickschema.org/schema/Brick/ref#",
                "sosa": "http://www.w3.org/ns/sosa/",
                "schema": "http://schema.org/",
            },
            "@id": "urn:building_1",
            "@type": "bc:Building",
            "schema:name": "Hospital Real",
            "schema:description": "This is Hospital Real, an ancient hospital in Granada built in 1504 following a design by Enrique Egas (late Gothic), although Charles V finished the project in Renaissance style. It is currently the main headquarters of the University of Granada, home to the Rectorate and other central university services. A more detailed description of the building is here: https://patrimonio.ugr.es/bien-inmueble/610/ To my best knowledge, the building has a permanent structural health monitoring system installed. Also, in November 2023, it was fully surveyed by researchers from University of Granada, and the natural vibration modes of the building were identified.",
            "schema:image": [
                {
                    "@type": "schema:ImageObject",
                    "schema:url": "https://upward-prime-turkey.ngrok-free.app/APIRest/HR/H%209.jpg",
                }
            ],
            "bc:yearBuilt": 1504,
            "bc:hasPart": {
                "@id": "urn:building_1:Tower_1",
                "@type": "bc:Tower",
                "bc:isMeteredBy": {
                    "@id": "urn:building_1:Tower_1-1",
                    "@type": ["bc:Tower"],
                    "schema:image": [
                        {
                            "@type": "schema:ImageObject",
                            "schema:url": "https://upward-prime-turkey.ngrok-free.app/APIRest/HR/IMG_20231125_103703.jpg",
                        }
                    ],
                    "bc:hasPoint": [
                        {
                            "@id": "urn:building_1:tower_1-1:sensor_1",
                            "@type": ["bc:Sensor", "sosa:Sensor"],
                            "bc:hasExternalReference": {
                                "@id": "urn:building_1:tower_1-1:sensor_1:ref",
                                "@type": "ref:TimeseriesReference",
                                "ref:hasTimeseriesId": "6ffe8f80-0782-452d-adf3-212daa712370",
                                "ref:storedAt": "https://upward-prime-turkey.ngrok-free.app/APIRest/plot/",
                            },
                            "sosa:madeObservation": {
                                "@id": "urn:building_1:tower_1-1:sensor_1:observation_1"
                            },
                        }
                    ],
                },
            },
        },
        {
            "@context": "http://schema.org",
            "@id": "urn:modal:data:2",
            "relatedTo": {"@id": "urn:building_1"},
            "buildingParts": [
                {
                    "name": "Tower",
                    "time": "19-05-2024",
                    "note": "daylight",
                    "modes": [
                        {
                            "name": "Mode 1",
                            "frequency": 3.773,
                            "damping": {"unit": "%", "value": 5.46},
                        },
                        {
                            "name": "Mode 2",
                            "frequency": 4.988,
                            "damping": {"unit": "%", "value": 3.682},
                        },
                    ],
                }
            ],
        },
    ],
    "health": [
        {
            "@context": {"schema": "http://schema.org/"},
            "@id": "http://example.org/medicalentity/diabetes",
            "@type": "schema:MedicalEntity",
            "schema:name": "Diabetes",
            "schema:description": "A group of diseases that result in too much sugar in the blood (high blood glucose).",
        },
        {
            "@context": {"schema": "http://schema.org/"},
            "@id": "http://example.org/medicalcondition/type2diabetes",
            "@type": "schema:MedicalCondition",
            "schema:name": "Type 2 Diabetes",
            "schema:description": "A chronic condition that affects the way the body processes blood sugar (glucose).",
            "schema:possibleTreatment": [
                {
                    "@id": "http://example.org/medicaltherapy/insulintherapy",
                    "@type": "schema:MedicalTherapy",
                    "schema:name": "Insulin Therapy",
                    "schema:description": "Insulin therapy is used to manage blood sugar levels.",
                }
            ],
            "schema:riskFactor": [
                {
                    "@id": "http://example.org/medicalriskfactor/obesity",
                    "@type": "schema:MedicalRiskFactor",
                    "schema:name": "Obesity",
                    "schema:description": "Excess body weight increases the risk of developing type 2 diabetes.",
                }
            ],
        },
        {
            "@context": {"schema": "http://schema.org/"},
            "@id": "http://example.org/medicaltest/bloodglucosetest",
            "@type": "schema:MedicalTest",
            "schema:name": "Blood Glucose Test",
            "schema:purpose": "To measure the amount of glucose in the blood.",
            "schema:procedure": "A blood sample is taken and analyzed.",
            "schema:result": "Normal, Prediabetes, Diabetes",
        },
        {
            "@context": {"schema": "http://schema.org/"},
            "@id": "http://example.org/medicalprocedure/gastricbypasssurgery",
            "@type": "schema:MedicalProcedure",
            "schema:name": "Gastric Bypass Surgery",
            "schema:description": "A surgery that helps with weight loss by changing how the stomach and small intestine handle the food you eat.",
            "schema:preparation": "Follow pre-surgery diet and instructions.",
            "schema:followUp": "Regular follow-up appointments and lifestyle changes.",
        },
        {
            "@context": {"schema": "http://schema.org/"},
            "@id": "http://example.org/medicaldevice/bloodglucosemeter",
            "@type": "schema:MedicalDevice",
            "schema:name": "Blood Glucose Meter",
            "schema:type": "Medical Device",
            "schema:manufacturer": {
                "@id": "http://example.org/organization/medicaldevicesinc",
                "@type": "schema:Organization",
                "schema:name": "Medical Devices Inc.",
                "schema:identifier": "MD123456",
                "schema:address": "123 Medical St, MedCity, MedCountry",
                "schema:contactPoint": "contact@medicaldevices.com",
            },
            "schema:intendedUse": "To measure blood glucose levels.",
        },
        {
            "@context": {"schema": "http://schema.org/"},
            "@id": "http://example.org/medicaltherapy/insulintherapy",
            "@type": "schema:MedicalTherapy",
            "schema:name": "Insulin Therapy",
            "schema:description": "Insulin therapy is used to manage blood sugar levels.",
            "schema:duration": "Ongoing",
            "schema:sideEffect": [
                {
                    "@id": "http://example.org/medicalsignorsymptom/hypoglycemia",
                    "@type": "schema:MedicalSignOrSymptom",
                    "schema:name": "Hypoglycemia",
                    "schema:description": "Low blood sugar levels.",
                }
            ],
        },
    ],
    "news": [
        {
            "@context": {"schema": "http://schema.org/"},
            "@id": "http://example.org/newsarticle/breakingnewsmajorevent",
            "@type": "schema:NewsArticle",
            "schema:headline": "Breaking News: Major Event Happens",
            "schema:datePublished": "2024-06-25",
            "schema:author": {"@id": "http://example.org/person/janejournalist"},
            "schema:articleBody": "This is the body of the breaking news article.",
            "schema:publisher": {"@id": "http://example.org/organization/newsagency"},
        },
        {
            "@context": {"schema": "http://schema.org/"},
            "@id": "http://example.org/person/janejournalist",
            "@type": "schema:Person",
            "schema:name": "Jane Journalist",
            "schema:contactPoint": "jane@example.com",
            "schema:address": "123 News St, Media City, News Country",
        },
        {
            "@context": {"schema": "http://schema.org/"},
            "@id": "http://example.org/organization/newsagency",
            "@type": "schema:Organization",
            "schema:name": "News Agency",
            "schema:contactPoint": "contact@newsagency.com",
            "schema:address": "456 Media Ave, Media City, News Country",
        },
        {
            "@context": {"schema": "http://schema.org/"},
            "@id": "http://example.org/event/pressconference",
            "@type": "schema:Event",
            "schema:name": "Press Conference",
            "schema:startDate": "2024-06-26",
            "schema:endDate": "2024-06-26",
            "schema:location": {"@id": "http://example.org/place/conferencehall"},
            "schema:organizer": {"@id": "http://example.org/organization/newsagency"},
        },
        {
            "@context": {"schema": "http://schema.org/"},
            "@id": "http://example.org/place/conferencehall",
            "@type": "schema:Place",
            "schema:name": "Conference Hall",
            "schema:address": "789 Conference Rd, Media City, News Country",
            "schema:geo": {
                "@type": "schema:GeoCoordinates",
                "schema:latitude": "12.345678",
                "schema:longitude": "98.765432",
            },
        },
    ],
    "SPAR": [
        {
            "@context": {
                "fabio": "http://purl.org/spar/fabio/",
                "dcterms": "http://purl.org/dc/terms/",
                "foaf": "http://xmlns.com/foaf/0.1/",
                "cito": "http://purl.org/spar/cito/",
            },
            "@id": "http://example.org/researchpaper/innovativemethodsai",
            "@type": "fabio:ResearchPaper",
            "dcterms:title": "Innovative Methods in AI Research",
            "dcterms:creator": [
                {"@id": "http://example.org/person/dralice", "@type": "foaf:Person"},
                {"@id": "http://example.org/person/profbob", "@type": "foaf:Person"},
            ],
            "dcterms:abstract": "This paper explores innovative methods in artificial intelligence research.",
            "dcterms:date": "2024-06-25",
            "dcterms:publisher": {
                "@id": "http://example.org/organization/techsciencepublishers"
            },
            "fabio:hasJournalVolume": "15",
            "fabio:hasJournalIssue": "2",
            "fabio:hasPageNumbers": "45-60",
            "dcterms:identifier": "https://doi.org/10.1234/5678",
            "dcterms:rights": "All rights reserved.",
            "cito:cites": {"@id": "http://example.org/researchpaper/previousstudyai"},
        },
        {
            "@context": {
                "fabio": "http://purl.org/spar/fabio/",
                "dcterms": "http://purl.org/dc/terms/",
                "foaf": "http://xmlns.com/foaf/0.1/",
                "cito": "http://purl.org/spar/cito/",
            },
            "@id": "http://example.org/researchpaper/previousstudyai",
            "@type": "fabio:ResearchPaper",
            "dcterms:title": "Previous Study on AI",
            "dcterms:creator": [
                {"@id": "http://example.org/person/dralice", "@type": "foaf:Person"},
                {"@id": "http://example.org/person/profbob", "@type": "foaf:Person"},
            ],
            "dcterms:abstract": "This paper discusses the previous study on artificial intelligence.",
            "dcterms:date": "2023-05-15",
            "dcterms:publisher": {
                "@id": "http://example.org/organization/techsciencepublishers"
            },
            "fabio:hasJournalVolume": "12",
            "fabio:hasJournalIssue": "1",
            "fabio:hasPageNumbers": "30-40",
            "dcterms:identifier": "https://doi.org/10.9876/5432",
            "dcterms:rights": "All rights reserved.",
        },
        {
            "@context": {"schema": "http://schema.org/"},
            "@id": "http://example.org/person/dralice",
            "@type": "schema:Person",
            "schema:name": "Dr. Alice",
        },
        {
            "@context": {"schema": "http://schema.org/"},
            "@id": "http://example.org/person/profbob",
            "@type": "schema:Person",
            "schema:name": "Prof. Bob",
        },
        {
            "@context": {"schema": "http://schema.org/"},
            "@id": "http://example.org/organization/techsciencepublishers",
            "@type": "schema:Organization",
            "schema:name": "Tech Science Publishers",
            "schema:contactPoint": "contact@techscience.com",
            "schema:address": "123 Tech St, Innovation City, Science Country",
        },
    ],
}

vectorize_jsonld_examples = [
    {
        "@context": {
            "bibo": "http://purl.org/ontology/bibo/",
            "dc": "http://purl.org/dc/elements/1.1/",
            "dcterms": "http://purl.org/dc/terms/",
            "foaf": "http://xmlns.com/foaf/0.1/",
            "xsd": "http://www.w3.org/2001/XMLSchema#",
        },
        "@id": "http://example.org/reports/FIN-2024-001",
        "@type": "bibo:Report",
        "dc:title": "Construction and Maintenance Report",
        "dc:identifier": "FIN-2024-001",
        "dcterms:issued": {"@type": "xsd:date", "@value": "2024-06-30"},
        "dc:publisher": {
            "@id": "http://example.org/organizations/CityOfParis",
            "@type": "foaf:Organization",
            "foaf:name": "City of Paris",
        },
        "bibo:DocumentPart": [
            {
                "@type": "bibo:DocumentPart",
                "dcterms:title": "Related Building",
                "bibo:content": "Building Name: Eiffel Tower, Address: Champ de Mars, 5 Avenue Anatole France, 75007 Paris, France, Owner: City of Paris",
            },
            {
                "@type": "bibo:DocumentPart",
                "dcterms:title": "Financial Details",
                "bibo:content": 'Construction Costs: [1887: 1000000 FRF, "Initial construction costs for the foundation"], [1888: 2000000 FRF, "Construction costs for the first and second levels"], [1889: 1500000 FRF, "Final construction costs for the completion of the tower"]. Maintenance Costs: [2022: 500000 EUR, "Annual maintenance and painting"], [2023: 600000 EUR, "Structural inspections and minor repairs"]. Funding Sources: [Government Grant: 2000000 EUR, "Annual grant for maintenance and operations"], [Tourism Revenue: 1000000 EUR, "Revenue from ticket sales and concessions"]',
            },
        ],
    },
    {
        "@context": {
            "bc": "https://brickschema.org/schema/Brick#",
            "schema": "http://schema.org/",
        },
        "@type": "bc:Sensor",
        "@id": "urn:sensor_temperature_eiffel_tower",
        "schema:name": "Temperature Sensor",
        "bc:type": "temperature",
        "bc:measures": "temperature",
        "bc:installedAt": {"@id": "urn:buildingcomponent_first_floor"},
        "bc:status": "active",
    },
    {
        "@context": {
            "bc": "https://brickschema.org/schema/Brick#",
            "schema": "http://schema.org/",
        },
        "@id": "urn:facade_2",
        "@type": "bc:Outside",
        "schema:name": "Hospital Real Facade - building diagnosis and material study",
        "bc:isAssociatedWith": {
            "@id": "urn:building_1",
            "schema:isPartOf": "did:dkg:otp:2043/0x5cac41237127f94c2d21dae0b14bfefa99880630/6322528",
        },
        "schema:description": "This is a building diagnosis and material study for Hospital Real, an ancient hospital in Granada built in 1504 following a design by Enrique Egas (late Gothic), although Charles V finished the project in Renaissance style. It is currently the main headquarters of the University of Granada, home to the Rectorate and other central university services. A more detailed description of the building is here: https://patrimonio.ugr.es/bien-inmueble/610/ To my best knowledge, the building has a permanent structural health monitoring system installed. Also, in November 2023, it was fully surveyed by researchers from University of Granada, and the natural vibration modes of the building were identified.",
        "schema:author": {
            "@type": "schema:Person",
            "schema:name": "Javier Gallego Roca",
        },
        "schema:image": [
            {
                "@type": "schema:ImageObject",
                "schema:url": "https://res.cloudinary.com/dhupiskro/image/upload/fl_preserve_transparency/v1717661968/facade1_cfau5o.jpg?_s=public-apps",
            }
        ],
        "schema:material": [
            {
                "@type": "schema:Material",
                "schema:name": "Travertine",
                "schema:description": "Material used on the main side",
            },
            {
                "@type": "schema:Material",
                "schema:name": "White Macael marble",
                "schema:description": "Material used on the main side",
            },
        ],
    },
]
