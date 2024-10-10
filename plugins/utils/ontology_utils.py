def get_ontology_by_name(selected_ontology):
    if selected_ontology == "BIBO":
        return """
        Bibliographic Ontology (BIBO) is an ontology for describing bibliographic resources on the semantic web. It supports citation, document classification, and describing various kinds of documents in RDF.

        ** Key Classes and Properties **
        1) Document

        Represents any bibliographic document.
        Properties:
        identifier (string): Unique identifier.
        title (string): Title of the document.
        abstract (string): Summary of the document.
        authorList (array of Persons or Organizations): List of authors.
        editorList (array of Persons or Organizations): List of editors.
        issued (date): Date of publication.
        pageStart (integer): Starting page.
        pageEnd (integer): Ending page.
        volume (integer): Volume number.
        issue (integer): Issue number.
        doi (string): Digital Object Identifier.
        isbn (string): International Standard Book Number.
        issn (string): International Standard Serial Number.
        language (string): Language of the document.
        publisher (Organization): Publisher of the document.
        documentPart (DocumentPart which has fields title and content): Parts of the document, use to summarize the main points of the documents.
        
        2) Person

        Represents an individual associated with a document.
        Properties:
        name (string): Full name.
        givenName (string): First name.
        familyName (string): Last name.
        
        3) Organization

        Represents an organization associated with a document.
        Properties:
        name (string): Name of the organization.
        
        4) Event

        Represents events related to bibliographic resources.
        Properties:
        name (string): Name of the event.
        place (string): Location of the event.
        date (date): Date of the event.
        
        5) Book

        Represents a book.
        Properties:
        title (string): Title of the book.
        authorList (array of Persons or Organizations): List of authors.
        editorList (array of Persons or Organizations): List of editors.
        publisher (Organization): Publisher of the book.
        isbn (string): International Standard Book Number.
        
        6) Article

        Represents an article, typically published in a journal.
        Properties:
        title (string): Title of the article.
        authorList (array of Persons or Organizations): List of authors.
        journal (string): Journal name.
        volume (integer): Volume number.
        issue (integer): Issue number.
        pageStart (integer): Starting page.
        pageEnd (integer): Ending page.
        doi (string): Digital Object Identifier.
        
        7) Thesis

        Represents an academic thesis.
        Properties:
        title (string): Title of the thesis.
        author (Person): Author of the thesis.
        degree (string): Degree awarded for the thesis.
        institution (Organization): Awarding institution.
        
        8) Report

        Represents a report.
        Properties:
        title (string): Title of the report.
        authorList (array of Persons or Organizations): List of authors.
        publisher (Organization): Publisher of the report.
        issued (date): Date of publication.
        documentPart (DocumentPart which has fields title and content): Parts of the document, use to summarize the main points of the documents.

        9) Patent

        Represents a patent document.
        Properties:
        title (string): Title of the patent.
        inventorList (array of Persons): List of inventors.
        assignee (Organization): Assignee of the patent.
        patentNumber (string): Patent number.
        filingDate (date): Filing date of the patent.
        
        10) Magazine

        Represents a magazine.
        Properties:
        title (string): Title of the magazine.
        issue (integer): Issue number.
        volume (integer): Volume number.
        publisher (Organization): Publisher of the magazine.
        issn (string): International Standard Serial Number.
        
        11) Conference

        Represents a conference event.
        Properties:
        name (string): Name of the conference.
        location (string): Location of the conference.
        date (date): Date of the conference.
        organizer (Organization): Organizer of the conference.
        
        12) Webpage

        Represents a webpage.
        Properties:
        title (string): Title of the webpage.
        url (string): URL of the webpage.
        authorList (array of Persons or Organizations): List of authors.
        publisher (Organization): Publisher of the webpage.
        
        13) Journal

        Represents a journal.
        Properties:
        title (string): Title of the journal.
        issn (string): International Standard Serial Number.
        publisher (Organization): Publisher of the journal.
        
        14) LegalDocument

        Represents a legal document.
        Properties:
        title (string): Title of the legal document.
        court (Organization): Court associated with the document.
        date (date): Date of the document.
        
        15) Manuscript

        Represents an unpublished manuscript.
        Properties:
        title (string): Title of the manuscript.
        authorList (array of Persons or Organizations): List of authors.
        abstract (string): Summary of the manuscript.

        Example JSON Representation
        {
            "Document": {
                "identifier": "12345",
                "title": "Understanding BIBO",
                "abstract": "A summary of BIBO ontology.",
                "authorList": [
                {
                    "Person": {
                    "name": "John Doe",
                    "givenName": "John",
                    "familyName": "Doe"
                    }
                },
                {
                    "Organization": {
                    "name": "BIBO Research Group"
                    }
                }
                ],
                "editorList": [
                {
                    "Person": {
                    "name": "Jane Smith",
                    "givenName": "Jane",
                    "familyName": "Smith"
                    }
                }
                ],
                "issued": "2024-06-24",
                "pageStart": 1,
                "pageEnd": 10,
                "volume": 1,
                "issue": 1,
                "doi": "10.1234/bibo.2024",
                "isbn": "978-3-16-148410-0",
                "issn": "1234-5678",
                "language": "en",
                "publisher": {
                "Organization": {
                    "name": "BIBO Publishing"
                }
                }
            }
        }

        ** Instructions for LLM **

        Understanding Classes and Properties: Recognize key classes (Document, Person, Organization, Event, Book, Article, Thesis, Report, Patent, Magazine, Conference, Webpage, Journal, LegalDocument, Manuscript) and their properties.
        Hierarchical Structure: Understand how classes can be nested within each other (e.g., Document contains Person and Organization).
        Property Types: Identify types of properties (string, integer, date, array).
        Make sure to include as much information as you can from the input context. If you cannot find an ideal property for the information, use a generic one.
        """
    elif selected_ontology == "FOAF":
        return """
        FOAF (Friend of a Friend) is an ontology used to describe people, their activities, and their relations to other people and objects. It supports the creation of a web of machine-readable pages describing people, the links between them, and the things they create and do.

        ** Key Classes and Properties **
        1) Person

        Represents an individual person.
        Properties:
        name (string): Full name.
        givenName (string): First name.
        familyName (string): Last name.
        mbox (string): Email address.
        homepage (string): URL of the person's homepage.
        workplaceHomepage (string): URL of the person's workplace homepage.
        knows (array of Persons): List of people the person knows.
        
        2) Organization

        Represents an organization.
        Properties:
        name (string): Name of the organization.
        homepage (string): URL of the organization's homepage.
        
        3) Document

        Represents a document.
        Properties:
        title (string): Title of the document.
        creator (Person or Organization): The creator of the document.
        subject (string): Subject of the document.
        description (string): Description of the document.
        date (date): Date the document was created or published.
        format (string): File format of the document (e.g., PDF, HTML).
        language (string): Language of the document.
        license (string): License of the document.

        4) Image

        Represents an image.
        Properties:
        title (string): Title of the image.
        creator (Person or Organization): The creator of the image.
        subject (string): Subject of the image.
        description (string): Description of the image.
        date (date): Date the image was created or published.
        format (string): File format of the image (e.g., JPEG, PNG).
        language (string): Language of the image.
        license (string): License of the image.
        
        5) Project

        Represents a project.
        Properties:
        name (string): Name of the project.
        homepage (string): URL of the project's homepage.
        member (array of Persons or Organizations): Members of the project.

        6) Group

        Represents a group.
        Properties:
        name (string): Name of the group.
        member (array of Persons): Members of the group.

        7) SpatialThing

        Represents a spatial thing (a place).
        Properties:
        name (string): Name of the place.
        location (string): Location of the place.

        8) OnlineAccount

        Represents an online account.
        Properties:
        accountName (string): Name of the account.
        accountServiceHomepage (string): URL of the account service.
        accountOwner (Person): Owner of the account.

        9) PersonalProfileDocument

        Represents a personal profile document.
        Properties:
        maker (Person): Maker of the document.
        primaryTopic (Person): Primary topic of the document.

        Example JSON Representation
        {
            "Person": {
                "name": "John Doe",
                "givenName": "John",
                "familyName": "Doe",
                "mbox": "mailto:johndoe@example.com",
                "homepage": "http://www.johndoe.com",
                "workplaceHomepage": "http://www.example.com",
                "knows": [
                    {
                        "Person": {
                            "name": "Jane Smith",
                            "givenName": "Jane",
                            "familyName": "Smith",
                            "mbox": "mailto:janesmith@example.com"
                        }
                    },
                    {
                        "Person": {
                            "name": "Bob Johnson",
                            "givenName": "Bob",
                            "familyName": "Johnson",
                            "mbox": "mailto:bobjohnson@example.com"
                        }
                    }
                ],
                "organization": {
                    "Organization": {
                        "name": "Example Corp",
                        "homepage": "http://www.example.com"
                    }
                }
            }
        }

        ** Instructions for LLM **

        Understanding Classes and Properties: Recognize key classes (Person, Organization, Document, Image, Project) and their properties.
        Hierarchical Structure: Understand how classes can be nested within each other (e.g., Person knows other Persons, and belongs to Organizations).
        Property Types: Identify types of properties (string, array, date).
        """
    elif selected_ontology == "entertainment":
        return """
        The 'entertainment' custom ontology is designed to describe creative works, people, organizations, and other entities related to the entertainment industry, using schema.org types. This ontology supports the detailed representation of various entities and their interrelationships in the context of entertainment.

        ** Key Classes and Properties **
        
        1) CreativeWork

        Represents a creative work such as books, movies, music, or games.
        Properties:
        title (string): Title of the work.
        creator (Person or Organization): The creator of the work.
        datePublished (date): Date the work was published.
        genre (string): Genre of the work.
        description (string): Description of the work.
        keywords (array of strings): Keywords associated with the work.
        inLanguage (string): Language of the work.
        license (string): License of the work.

        2) Person

        Represents an individual person.
        Properties:
        name (string): Full name.
        givenName (string): First name.
        familyName (string): Last name.
        email (string): Email address.
        url (string): URL of the person's homepage.
        worksFor (Organization): Organization the person works for.
        knows (array of Persons): List of people the person knows.

        3) Organization

        Represents an organization.
        Properties:
        name (string): Name of the organization.
        url (string): URL of the organization's homepage.
        member (array of Persons): Members of the organization.

        4) Role

        Represents a role played by a person or organization.
        Properties:
        roleName (string): Name of the role.
        startDate (date): Start date of the role.
        endDate (date): End date of the role.
        actor (Person or Organization): The actor of the role.

        5) Event

        Represents an event.
        Properties:
        name (string): Name of the event.
        startDate (date): Start date of the event.
        endDate (date): End date of the event.
        location (Place): Location of the event.
        organizer (Person or Organization): Organizer of the event.

        6) Action

        Represents an action performed by a person or organization.
        Properties:
        actionName (string): Name of the action.
        startTime (date): Start time of the action.
        endTime (date): End time of the action.
        participant (Person or Organization): Participant of the action.

        7) Intangible

        Represents an intangible entity.
        Properties:
        name (string): Name of the intangible entity.

        8) Place

        Represents a place.
        Properties:
        name (string): Name of the place.
        address (string): Address of the place.
        geo (GeoCoordinates): Geographical coordinates of the place.

        9) Thing

        Represents a generic entity.
        Properties:
        name (string): Name of the entity.

        10) VideoGame (subclass of CreativeWork)

        Represents a video game.
        Properties:
        title (string): Title of the video game.
        developer (Organization): Developer of the video game.
        releaseDate (date): Release date of the video game.
        genre (string): Genre of the video game.
        gamePlatform (array of strings): Platforms the video game is available on.
        description (string): Description of the video game.
        character (array of Persons): Characters in the video game.

        11) ShortStory (subclass of CreativeWork)

        Represents a short story.
        Properties:
        title (string): Title of the short story.
        author (Person): Author of the short story.
        datePublished (date): Date the short story was published.
        genre (string): Genre of the short story.
        description (string): Description of the short story.

        Example JSON Representation
        {
            "VideoGame": {
                "title": "World of Warcraft",
                "developer": {
                "Organization": {
                    "name": "Blizzard Entertainment",
                    "url": "https://www.blizzard.com/"
                }
                },
                "releaseDate": "2004-11-23",
                "genre": ["MMORPG", "Fantasy", "Adventure"],
                "platform": ["PC", "Mac"],
                "description": "World of Warcraft is a massively multiplayer online role-playing game (MMORPG) set in the fantasy world of Azeroth.",
                "characters": [
                    {
                        "Person": {
                        "name": "Thrall",
                        "role": "Warchief",
                        "story": "Thrall is the former Warchief of the Horde and one of the most powerful shamans in Azeroth."
                        }
                    },
                    {
                        "Person": {
                        "name": "Jaina Proudmoore",
                        "role": "Archmage",
                        "story": "Jaina Proudmoore is a powerful sorceress and the leader of the Kirin Tor."
                        }
                    },
                    {
                        "Person": {
                        "name": "Sylvanas Windrunner",
                        "role": "Banshee Queen",
                        "story": "Sylvanas Windrunner is the former Ranger-General of Silvermoon and the current Warchief of the Horde."
                        }
                    }
                ]
            }
        }


        ** Instructions for LLM **

        Understanding Classes and Properties: Recognize key classes (CreativeWork, Person, Organization, Role, Event, Action, Intangible, Place, Thing, VideoGame, ShortStory) and their properties.
        Hierarchical Structure: Understand how classes can be nested within each other (e.g., VideoGame contains Persons as characters, and is developed by an Organization).
        Property Types: Identify types of properties (string, array, date).
        """
    elif selected_ontology == "GS1":
        return """
        The 'GS1' custom ontology is designed to describe entities and processes in the supply chain and inventory management sector. This ontology supports detailed representation of various entities, their attributes, and their interrelationships in the context of supply chain and inventory.

        ** Key Classes and Properties **
        
        1) Product

        Represents a product.
        Properties:
        name (string): Name of the product.
        identifier (string): Unique identifier for the product (e.g., GTIN).
        brand (string): Brand of the product.
        category (string): Category of the product.
        description (string): Description of the product.
        manufacturer (Organization): Organization that manufactures the product.
        weight (string): Weight of the product.
        dimensions (string): Dimensions of the product.
        image (string): URL of the product image.

        2) Organization

        Represents an organization involved in the supply chain.
        Properties:
        name (string): Name of the organization.
        identifier (string): Unique identifier for the organization (e.g., GLN).
        address (string): Address of the organization.
        contactPoint (string): Contact point at the organization.

        3) Inventory

        Represents inventory information for a product.
        Properties:
        product (Product): The product in the inventory.
        location (Place): Location of the inventory.
        quantity (integer): Quantity of the product in the inventory.
        status (string): Status of the inventory (e.g., in stock, out of stock).

        4) Shipment

        Represents a shipment of products.
        Properties:
        identifier (string): Unique identifier for the shipment (e.g., SSCC).
        shippedFrom (Organization): Organization that shipped the products.
        shippedTo (Organization): Organization receiving the products.
        items (array of Products): List of products in the shipment.
        shipmentDate (date): Date the shipment was made.
        estimatedArrival (date): Estimated arrival date of the shipment.
        status (string): Status of the shipment (e.g., in transit, delivered).

        5) Place

        Represents a place, such as a warehouse or store.
        Properties:
        name (string): Name of the place.
        address (string): Address of the place.
        geo (GeoCoordinates): Geographical coordinates of the place.

        6) TradeItem

        Represents an individual trade item.
        Properties:
        gtin (string): Global Trade Item Number.
        description (string): Description of the trade item.
        brandName (string): Brand name of the trade item.
        netWeight (string): Net weight of the trade item.
        netContent (string): Net content of the trade item.

        7) BusinessTransaction

        Represents a business transaction between organizations.
        Properties:
        transactionID (string): Unique identifier for the transaction.
        transactionDate (date): Date of the transaction.
        transactionType (string): Type of the transaction (e.g., purchase, sale).
        involvedParties (array of Organizations): Organizations involved in the transaction.

        8) Order

        Represents an order placed by an organization.
        Properties:
        orderID (string): Unique identifier for the order.
        orderDate (date): Date the order was placed.
        orderItems (array of TradeItems): List of trade items in the order.
        orderStatus (string): Status of the order (e.g., pending, fulfilled).

        Example JSON Representation
        {
            "Product": {
                "name": "Sample Product",
                "identifier": "01234567891234",
                "brand": "Sample Brand",
                "category": "Sample Category",
                "description": "This is a sample product description.",
                "manufacturer": {
                    "Organization": {
                        "name": "Sample Manufacturer",
                        "identifier": "1234567890123",
                        "address": "123 Sample St, Sample City, Sample Country",
                        "contactPoint": "contact@samplemanufacturer.com"
                    }
                },
                "weight": "1kg",
                "dimensions": "10x10x10 cm",
                "image": "http://example.com/sample-product.jpg"
            },
            "Inventory": {
                "product": {
                    "Product": {
                        "name": "Sample Product",
                        "identifier": "01234567891234"
                    }
                },
                "location": {
                    "Place": {
                        "name": "Sample Warehouse",
                        "address": "456 Sample Rd, Sample City, Sample Country",
                        "geo": "12.345678, 98.765432"
                    }
                },
                "quantity": 100,
                "status": "in stock"
            },
            "Shipment": {
                "identifier": "123456789012345678",
                "shippedFrom": {
                    "Organization": {
                        "name": "Sample Shipper",
                        "identifier": "9876543210987",
                        "address": "789 Sample Ave, Sample City, Sample Country"
                    }
                },
                "shippedTo": {
                    "Organization": {
                        "name": "Sample Receiver",
                        "identifier": "1234567890123",
                        "address": "321 Sample Blvd, Sample City, Sample Country"
                    }
                },
                "items": [
                    {
                        "Product": {
                            "name": "Sample Product",
                            "identifier": "01234567891234"
                        }
                    }
                ],
                "shipmentDate": "2024-06-24",
                "estimatedArrival": "2024-06-30",
                "status": "in transit"
            }
        }


        ** Instructions for LLM **

        Understanding Classes and Properties: Recognize key classes (Product, Organization, Inventory, Shipment, Place) and their properties.
        Hierarchical Structure: Understand how classes can be nested within each other (e.g., Product contains Organization as manufacturer, Shipment contains array of Products).
        Property Types: Identify types of properties (string, integer, date, array).
        """
    elif selected_ontology == "Brick" or selected_ontology == "SOSA":
        return """
        The 'Brick' ontology is designed to describe entities and processes in buildings, including their physical and operational aspects. This ontology supports detailed representation of various entities, their attributes, and their interrelationships in the context of buildings.

        ** Key Classes and Properties **

        1) Building

        Represents a building.
        Properties:
        name (string): Name of the building.
        address (string): Address of the building.
        owner (Organization): Organization that owns the building.
        hasPart (array of BuildingComponents): Components that are part of the building.

        2) BuildingComponent

        Represents a component of a building.
        Properties:
        name (string): Name of the component.
        type (string): Type of the component (e.g., room, floor, HVAC system).
        serves (BuildingComponent): Other components served by this component.
        location (Place): Physical location of the component within the building.
        installedBy (Organization): Organization that installed the component.
        hasSubComponent (array of BuildingComponents): Sub-components that are part of this component.
        madeOf (array of Materials): Materials used in the building component.

        3) Sensor

        Represents a sensor installed in a building.
        Properties:
        name (string): Name of the sensor.
        type (string): Type of the sensor (e.g., temperature, humidity).
        measures (string): Parameter measured by the sensor (e.g., temperature, humidity).
        installedAt (BuildingComponent): Building component where the sensor is installed.
        status (string): Operational status of the sensor (e.g., active, inactive).

        4) Organization

        Represents an organization involved in the building's lifecycle.
        Properties:
        name (string): Name of the organization.
        identifier (string): Unique identifier for the organization.
        address (string): Address of the organization.
        contactPoint (string): Contact point at the organization.

        5) Place

        Represents a place within a building.
        Properties:
        name (string): Name of the place.
        address (string): Address of the place.
        geo (GeoCoordinates): Geographical coordinates of the place.
        
        6) GeoCoordinates

        Represents geographical coordinates.
        Properties:
        latitude (string): Latitude of the place.
        longitude (string): Longitude of the place.

        7) Material

        Represents a material used in the construction of a building component.
        Properties:
        name (string): Name of the material.
        type (string): Type of the material (e.g., concrete, steel, wood).
        supplier (Organization): Organization that supplies the material.

        ** Example JSON Representation **
        {
            "Building": {
                "name": "Sample Building",
                "address": "123 Sample St, Sample City, Sample Country",
                "owner": {
                    "Organization": {
                        "name": "Sample Owner",
                        "identifier": "1234567890123",
                        "address": "123 Owner St, Sample City, Sample Country",
                        "contactPoint": "contact@sampleowner.com"
                    }
                },
                "hasPart": [
                    {
                        "BuildingComponent": {
                            "name": "Sample Floor",
                            "type": "floor",
                            "serves": null,
                            "location": {
                                "Place": {
                                    "name": "Sample Location",
                                    "address": "Sample Address",
                                    "geo": {
                                        "GeoCoordinates": {
                                            "latitude": "12.345678",
                                            "longitude": "98.765432"
                                        }
                                    }
                                }
                            },
                            "installedBy": {
                                "Organization": {
                                    "name": "Sample Installer",
                                    "identifier": "9876543210987",
                                    "address": "789 Installer Ave, Sample City, Sample Country",
                                    "contactPoint": "contact@sampleinstaller.com"
                                }
                            },
                            "hasSubComponent": [
                                {
                                    "BuildingComponent": {
                                        "name": "Sample Room",
                                        "type": "room",
                                        "serves": null,
                                        "location": {
                                            "Place": {
                                                "name": "Sample Room Location",
                                                "address": "Sample Room Address",
                                                "geo": {
                                                    "GeoCoordinates": {
                                                        "latitude": "12.345678",
                                                        "longitude": "98.765432"
                                                    }
                                                }
                                            }
                                        },
                                        "installedBy": {
                                            "Organization": {
                                                "name": "Sample Installer",
                                                "identifier": "9876543210987",
                                                "address": "789 Installer Ave, Sample City, Sample Country",
                                                "contactPoint": "contact@sampleinstaller.com"
                                            }
                                        },
                                        "madeOf": [
                                            {
                                                "Material": {
                                                    "name": "Concrete",
                                                    "type": "concrete",
                                                    "supplier": {
                                                        "Organization": {
                                                            "name": "Sample Supplier",
                                                            "identifier": "1234567890123",
                                                            "address": "123 Supplier St, Sample City, Sample Country",
                                                            "contactPoint": "contact@samplesupplier.com"
                                                        }
                                                    }
                                                }
                                            },
                                            {
                                                "Material": {
                                                    "name": "Steel",
                                                    "type": "steel",
                                                    "supplier": {
                                                        "Organization": {
                                                            "name": "Sample Supplier",
                                                            "identifier": "1234567890123",
                                                            "address": "123 Supplier St, Sample City, Sample Country",
                                                            "contactPoint": "contact@samplesupplier.com"
                                                        }
                                                    }
                                                }
                                            }
                                        ]
                                    }
                                }
                            ]
                        }
                    }
                ]
            },
            "Sensor": {
                "name": "Temperature Sensor",
                "type": "temperature",
                "measures": "temperature",
                "installedAt": {
                    "BuildingComponent": {
                        "name": "Sample Room",
                        "type": "room"
                    }
                },
                "status": "active"
            }
        }

        ** Instructions for LLM **

        Understanding Classes and Properties: Recognize key classes (Building, BuildingComponent, Sensor, Organization, Place, GeoCoordinates, Material) and their properties.
        Hierarchical Structure: Understand how classes can be nested within each other (e.g., Building contains BuildingComponent, BuildingComponent contains sub-components, BuildingComponent is made of Materials).
        Property Types: Identify types of properties (string, integer, array).
        """
    elif selected_ontology == "health":
        return """The 'health' ontology is designed to describe entities and processes in the healthcare sector, including medical entities, conditions, procedures, and related information. This ontology supports detailed representation of various entities, their attributes, and their interrelationships in the context of healthcare.

        ** Key Classes and Properties **

        1) MedicalEntity

        Represents a generic medical entity.
        Properties:
        name (string): Name of the medical entity.
        description (string): Description of the medical entity.

        2) MedicalCondition

        Represents a medical condition.
        Properties:
        name (string): Name of the medical condition.
        description (string): Description of the condition.
        possibleTreatment (array of MedicalTherapies): Possible treatments for the condition.
        riskFactor (array of MedicalRiskFactors): Risk factors associated with the condition.

        3) MedicalTest

        Represents a medical test.
        Properties:
        name (string): Name of the test.
        purpose (string): Purpose of the test.
        procedure (string): Procedure of the test.
        result (string): Result of the test.

        4) MedicalProcedure

        Represents a medical procedure.
        Properties:
        name (string): Name of the procedure.
        description (string): Description of the procedure.
        preparation (string): Preparation required for the procedure.
        followUp (string): Follow-up care required after the procedure.

        5) MedicalDevice

        Represents a medical device.
        Properties:
        name (string): Name of the device.
        type (string): Type of the device.
        manufacturer (Organization): Organization that manufactures the device.
        intendedUse (string): Intended use of the device.

        6) MedicalTherapy

        Represents a medical therapy.
        Properties:
        name (string): Name of the therapy.
        description (string): Description of the therapy.
        duration (string): Duration of the therapy.
        sideEffect (array of MedicalSignsOrSymptoms): Possible side effects of the therapy.

        7) Patient

        Represents a patient.
        Properties:
        name (string): Name of the patient.
        age (integer): Age of the patient.
        gender (string): Gender of the patient.
        medicalHistory (array of MedicalConditions): Medical history of the patient.

        8) Person

        Represents a generic person.
        Properties:
        name (string): Name of the person.
        contact (string): Contact details of the person.
        address (string): Address of the person.

        9) Hospital

        Represents a hospital.
        Properties:
        name (string): Name of the hospital.
        address (string): Address of the hospital.
        contact (string): Contact details of the hospital.
        departments (array of MedicalEntities): Departments within the hospital.

        10) Pharmacy

        Represents a pharmacy.
        Properties:
        name (string): Name of the pharmacy.
        address (string): Address of the pharmacy.
        contact (string): Contact details of the pharmacy.

        11) MedicalSignOrSymptom

        Represents a medical sign or symptom.
        Properties:
        name (string): Name of the sign or symptom.
        description (string): Description of the sign or symptom.

        12) MedicalRiskFactor

        Represents a medical risk factor.
        Properties:
        name (string): Name of the risk factor.
        description (string): Description of the risk factor.

        13) Drug

        Represents a drug.
        Properties:
        name (string): Name of the drug.
        description (string): Description of the drug.
        manufacturer (Organization): Organization that manufactures the drug.
        prescriptionStatus (DrugPrescriptionStatus): Prescription status of the drug.

        14) DrugPrescriptionStatus

        Represents the prescription status of a drug.
        Properties:
        status (string): Prescription status of the drug (e.g., over-the-counter, prescription only).

        ** Example JSON Representation **
        {
            "MedicalEntity": {
                "name": "Diabetes",
                "description": "A group of diseases that result in too much sugar in the blood (high blood glucose)."
            },
            "MedicalCondition": {
                "name": "Type 2 Diabetes",
                "description": "A chronic condition that affects the way the body processes blood sugar (glucose).",
                "possibleTreatment": [
                    {
                        "MedicalTherapy": {
                            "name": "Insulin Therapy",
                            "description": "Insulin therapy is used to manage blood sugar levels."
                        }
                    }
                ],
                "riskFactor": [
                    {
                        "MedicalRiskFactor": {
                            "name": "Obesity",
                            "description": "Excess body weight increases the risk of developing type 2 diabetes."
                        }
                    }
                ]
            },
            "MedicalTest": {
                "name": "Blood Glucose Test",
                "purpose": "To measure the amount of glucose in the blood.",
                "procedure": "A blood sample is taken and analyzed.",
                "result": "Normal, Prediabetes, Diabetes"
            },
            "MedicalProcedure": {
                "name": "Gastric Bypass Surgery",
                "description": "A surgery that helps with weight loss by changing how the stomach and small intestine handle the food you eat.",
                "preparation": "Follow pre-surgery diet and instructions.",
                "followUp": "Regular follow-up appointments and lifestyle changes."
            },
            "MedicalDevice": {
                "name": "Blood Glucose Meter",
                "type": "Medical Device",
                "manufacturer": {
                    "Organization": {
                        "name": "Medical Devices Inc.",
                        "identifier": "MD123456",
                        "address": "123 Medical St, MedCity, MedCountry",
                        "contactPoint": "contact@medicaldevices.com"
                    }
                },
                "intendedUse": "To measure blood glucose levels."
            },
            "MedicalTherapy": {
                "name": "Insulin Therapy",
                "description": "Insulin therapy is used to manage blood sugar levels.",
                "duration": "Ongoing",
                "sideEffect": [
                    {
                        "MedicalSignOrSymptom": {
                            "name": "Hypoglycemia",
                            "description": "Low blood sugar levels."
                        }
                    }
                ]
            },
            "Patient": {
                "name": "John Doe",
                "age": 45,
                "gender": "Male",
                "medicalHistory": [
                    {
                        "MedicalCondition": {
                            "name": "Hypertension",
                            "description": "High blood pressure."
                        }
                    }
                ]
            },
            "Hospital": {
                "name": "Central Hospital",
                "address": "789 Hospital St, HealthCity, HealthCountry",
                "contact": "contact@centralhospital.com"
            },
            "Pharmacy": {
                "name": "Health Pharmacy",
                "address": "101 Pharmacy St, MedCity, MedCountry",
                "contact": "contact@healthpharmacy.com"
            },
            "MedicalSignOrSymptom": {
                "name": "Headache",
                "description": "Pain in the head or upper neck."
            },
            "MedicalRiskFactor": {
                "name": "Smoking",
                "description": "Smoking increases the risk of developing various diseases."
            },
            "Drug": {
                "name": "Metformin",
                "description": "Medication used to treat type 2 diabetes.",
                "manufacturer": {
                    "Organization": {
                        "name": "Pharma Inc.",
                        "identifier": "PH123456",
                        "address": "202 Pharma St, PharmaCity, PharmaCountry"                    }
                },
                "prescriptionStatus": {
                    "DrugPrescriptionStatus": {
                        "status": "Prescription Only"
                    }
                }
            }
        }

        ** Instructions for LLM **

        Understanding Classes and Properties: Recognize key classes (MedicalEntity, MedicalCondition, MedicalTest, MedicalProcedure, MedicalDevice, MedicalTherapy, Patient, Person, HealthPlan, Place, Hospital, Pharmacy, MedicalSignOrSymptom, MedicalRiskFactor, MedicalRiskEstimator, Drug, DrugPrescriptionStatus) and their properties.
        Hierarchical Structure: Understand how classes can be nested within each other (e.g., MedicalCondition contains MedicalTherapies, MedicalTherapies contain side effects as MedicalSignsOrSymptoms).
        Property Types: Identify types of properties (string, integer, date, array).
        """
    elif selected_ontology == "news":
        return """The 'news' ontology is designed to describe entities and processes in the news and media sector, including articles, reports, organizations, people, and related information. This ontology supports detailed representation of various entities, their attributes, and their interrelationships in the context of news and media.

    ** Key Classes and Properties **

    1) CreativeWork

    Represents a generic creative work.
    Properties:
    name (string): Name of the creative work.
    author (Person or Organization): Author of the creative work.
    datePublished (date): Date when the work was published.
    description (string): Description of the creative work.
    publisher (Organization): Organization that published the work.

    2) NewsArticle

    Represents a news article.
    Properties:
    headline (string): Headline of the news article.
    datePublished (date): Date when the article was published.
    author (Person or Organization): Author of the article.
    articleBody (string): Body content of the article.
    publisher (Organization): Organization that published the article.

    3) Article

    Represents a generic article.
    Properties:
    headline (string): Headline of the article.
    datePublished (date): Date when the article was published.
    author (Person or Organization): Author of the article.
    articleBody (string): Body content of the article.
    publisher (Organization): Organization that published the article.

    4) Report

    Represents a report.
    Properties:
    name (string): Name of the report.
    author (Person or Organization): Author of the report.
    datePublished (date): Date when the report was published.
    description (string): Description of the report.
    publisher (Organization): Organization that published the report.

    5) Person

    Represents a person.
    Properties:
    name (string): Name of the person.
    contactPoint (string): Contact details of the person.
    address (string): Address of the person.

    6) Organization

    Represents an organization.
    Properties:
    name (string): Name of the organization.
    contactPoint (string): Contact details of the organization.
    address (string): Address of the organization.

    7) Event

    Represents an event.
    Properties:
    name (string): Name of the event.
    startDate (date): Start date of the event.
    endDate (date): End date of the event.
    location (Place): Location of the event.
    organizer (Person or Organization): Organizer of the event.

    8) MediaObject

    Represents a media object.
    Properties:
    name (string): Name of the media object.
    contentUrl (string): URL of the media content.
    encodingFormat (string): Format of the media content.

    9) AudioObject

    Represents an audio object.
    Properties:
    name (string): Name of the audio object.
    contentUrl (string): URL of the audio content.
    encodingFormat (string): Format of the audio content.
    duration (string): Duration of the audio content.

    10) VideoObject

    Represents a video object.
    Properties:
    name (string): Name of the video object.
    contentUrl (string): URL of the video content.
    encodingFormat (string): Format of the video content.
    duration (string): Duration of the video content.

    11) Comment

    Represents a comment.
    Properties:
    author (Person): Author of the comment.
    dateCreated (date): Date when the comment was created.
    text (string): Text content of the comment.

    12) Review

    Represents a review.
    Properties:
    author (Person): Author of the review.
    datePublished (date): Date when the review was published.
    reviewBody (string): Body content of the review.
    reviewRating (integer): Rating given in the review.

    13) Place

    Represents a place.
    Properties:
    name (string): Name of the place.
    address (string): Address of the place.
    geo (GeoCoordinates): Geographical coordinates of the place.

    14) Thing

    Represents a generic thing.
    Properties:
    name (string): Name of the thing.
    description (string): Description of the thing.

    ** Example JSON Representation **
    {
        "NewsArticle": {
            "headline": "Breaking News: Major Event Happens",
            "datePublished": "2024-06-25",
            "author": {
                "Person": {
                    "name": "Jane Journalist",
                    "contactPoint": "jane@example.com",
                    "address": "123 News St, Media City, News Country"
                }
            },
            "articleBody": "This is the body of the breaking news article.",
            "publisher": {
                "Organization": {
                    "name": "News Agency",
                    "contactPoint": "contact@newsagency.com",
                    "address": "456 Media Ave, Media City, News Country"
                }
            }
        },
        "Person": {
            "name": "Jane Journalist",
            "contactPoint": "jane@example.com",
            "address": "123 News St, Media City, News Country"
        },
        "Organization": {
            "name": "News Agency",
            "contactPoint": "contact@newsagency.com",
            "address": "456 Media Ave, Media City, News Country"
        },
        "Event": {
            "name": "Press Conference",
            "startDate": "2024-06-26",
            "endDate": "2024-06-26",
            "location": {
                "Place": {
                    "name": "Conference Hall",
                    "address": "789 Conference Rd, Media City, News Country",
                    "geo": {
                        "GeoCoordinates": {
                            "latitude": "12.345678",
                            "longitude": "98.765432"
                        }
                    }
                }
            },
            "organizer": {
                "Organization": {
                    "name": "News Agency",
                    "contactPoint": "contact@newsagency.com",
                    "address": "456 Media Ave, Media City, News Country"
                }
            }
        }
    }

    ** Instructions for LLM **

    Understanding Classes and Properties: Recognize key classes (CreativeWork, NewsArticle, Article, Report, Person, Organization, Event, MediaObject, AudioObject, VideoObject, Comment, Review, Place, Thing) and their properties.
    Hierarchical Structure: Understand how classes can be nested within each other (e.g., NewsArticle contains author as Person or Organization, Event contains location as Place).
    Property Types: Identify types of properties (string, integer, date, array).
    """
    elif selected_ontology == "SPAR":
        return """The 'SPAR' ontology is designed to describe entities and processes in the scientific research sector, including research papers, creators, citations, and related information. This ontology supports detailed representation of various entities, their attributes, and their interrelationships in the context of scientific research.

        ** Key Classes and Properties **

        1) ResearchPaper

        Represents a research paper.
        Properties:
        title (string): Title of the research paper.
        creator (array of Persons): Creators of the research paper.
        abstract (string): Abstract of the research paper.
        date (date): Date when the paper was published.
        publisher (Organization): Organization that published the paper.
        hasJournalVolume (string): Journal volume in which the paper was published.
        hasJournalIssue (string): Journal issue in which the paper was published.
        hasPageNumbers (string): Page numbers of the paper.
        identifier (string): Identifier of the paper (e.g., DOI).
        rights (string): Rights associated with the paper.
        hasPart (array of ResearchPaperParts): Parts of the research paper.
        roleIn (array of Roles): Roles associated with the research paper.
        cites: Citations from the paper.

        2) Person

        Represents a person.
        Properties:
        name (string): Name of the person.

        3) Organization

        Represents an organization.
        Properties:
        name (string): Name of the organization.
        identifier (string): Unique identifier for the organization.
        address (string): Address of the organization.
        contactPoint (string): Contact point at the organization.

        4) ResearchPaperPart

        Represents a part of a research paper.
        Properties:
        name (string): Name of the part.
        description (string): Description of the part

        ** Example JSON Representation **
        {
            "ResearchPaper": {
                "title": "Preliminary Study on X",
                "creator": [
                    {
                        "Person": {
                            "name": "Dr. A"
                        }
                    },
                    {
                        "Person": {
                            "name": "Prof. B"
                        }
                    }
                ],
                "abstract": "This study investigates...",
                "date": "2024-06-25",
                "publisher": {
                    "Organization": {
                        "name": "Science Publisher",
                        "contactPoint": "contact@sciencepublisher.com",
                        "address": "123 Science St, Research City, Science Country"
                    }
                },
                "hasJournalVolume": "12",
                "hasJournalIssue": "3",
                "hasPageNumbers": "123-130",
                "identifier": "https://doi.org/ID",
                "rights": "All rights reserved.",
                "hasPart": [
                    {
                        "ResearchPaperPart": {
                            "name": "Introduction",
                            "description": "Introduction to the study."
                        }
                    },
                    {
                        "ResearchPaperPart": {
                            "name": "Methodology",
                            "description": "Description of the methods used."
                        }
                    }
                ],
                cites: ["Paper 1"]
            }
        }

        ** Instructions for LLM **

        Understanding Classes and Properties: Recognize key classes (ResearchPaper, Person, Organization, Citation, ResearchPaperPart, Role) and their properties.
        Hierarchical Structure: Understand how classes can be nested within each other (e.g., ResearchPaper contains creators as Persons, ResearchPaper has parts as ResearchPaperParts).
        Property Types: Identify types of properties (string, integer, date, array).
        """


category_ontology_map = {
    "Finance": ["FIBO"],
    "Social Media": ["FOAF"],
    "Construction": ["Brick", "SOSA", "BIBO"],
    "Documentation": ["BIBO"],
    "Marketing": ["BIBO", "FOAF"],
    "Books": ["BIBO"],
    "Inventory": ["GS1"],
    "Entertainment": ["entertainment"],
    "Science": ["SPAR"],
    "Health": ["health"],
    "Supply Chain": ["GS1"],
    "News": ["news"],
}
