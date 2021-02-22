# JsonataStage

Query and manipulate json data with JSONata for DataStage

[[_TOC_]]

## Introduction

JSONata is a "JSON query and transformation language", see more and documentation at [jsonata.org](https://jsonata.org/).
This language allows easy querying and manipulate of json data and was ported to Java by IBM: (https://github.com/IBM/JSONata4Java)

This project is the implementation of this java library for usage in the Java Transformer stage in IBM DataStage.

## Installation

1. You can either build from source with maven or ant, or download the packaged jar file with all the dependencies from here: https://gitlab.com/nsitbim/satellites/jsonataStage/-/packages.
Or get the lastest release directly: [jsonata-1.0.0.jar](https://gitlab.com/nsitbim/satellites/jsonataStage/-/package_files/7330444/download)

2. Copy the jar file on an accessible path on the engine server.

## Usage

1. Design a paralell job and add the Java Integration stage. The stage must have one input and one output.
2. Add the path to the library the Java Classpath options
3. Add the User Class com.ibm.is.cc.javastage.jsonata.jsonata
4. Specify the options:
    * jsondata : input field holding valid json data
    * query : jsonata expression to query the input json
    * output: output field to write the result
    * serialize: true to serialize the result if it is an objet
    * expand: true to expand the result from array to records
5. Optionaly define output metadata: the stage has RCP capability, meaning that if RCP is enabled the input fields and newly created output defined will be propagated.

**To serialize an object, the output fields must be defined, if the mapping is direct the stage will populate the fields automatically, otherwise the path must be defined in the derivation of the field, see examples below. Please note that those operation are performed against the result of the query.**


![jsonata](https://res.cloudinary.com/dmeujvgly/image/upload/v1613989939/jsonata.png)

## Examples

### Perform a JSONata query

* Input field holding json data:
````json
{
	"Account":{
		"Account Name":"Firefly",
		"Order":[
			{
				"OrderID":"order103",
				"Product":[
					{
						"Product Name":"Bowler Hat",
						"ProductID":858383,
						"SKU":"0406654608",
						"Description":{
							"Colour":"Purple",
							"Width":300,
							"Height":200,
							"Depth":210,
							"Weight":0.75
						},
						"Price":34.45,
						"Quantity":2
					},
					{
						"Product Name":"Trilby hat",
						"ProductID":858236,
						"SKU":"0406634348",
						"Description":{
							"Colour":"Orange",
							"Width":300,
							"Height":200,
							"Depth":210,
							"Weight":0.6
						},
						"Price":21.67,
						"Quantity":1
					}
				]
			},
			{
				"OrderID":"order104",
				"Product":[
					{
						"Product Name":"Bowler Hat",
						"ProductID":858383,
						"SKU":"040657863",
						"Description":{
							"Colour":"Purple",
							"Width":300,
							"Height":200,
							"Depth":210,
							"Weight":0.75
						},
						"Price":34.45,
						"Quantity":4
					},
					{
						"ProductID":345664,
						"SKU":"0406654603",
						"Product Name":"Cloak",
						"Description":{
							"Colour":"Black",
							"Width":30,
							"Height":20,
							"Depth":210,
							"Weight":2
						},
						"Price":107.99,
						"Quantity":1
					}
				]
			}
		]
	}
}
````
* query: Account.Order.Product
* serialize: false
* expand: false
* Result in output field:
````json
[
	{
		"Product Name":"Bowler Hat",
		"ProductID":858383,
		"SKU":"0406654608",
		"Description":{
			"Colour":"Purple",
			"Width":300,
			"Height":200,
			"Depth":210,
			"Weight":0.75
		},
		"Price":34.45,
		"Quantity":2
	},
	{
		"Product Name":"Trilby hat",
		"ProductID":858236,
		"SKU":"0406634348",
		"Description":{
			"Colour":"Orange",
			"Width":300,
			"Height":200,
			"Depth":210,
			"Weight":0.6
		},
		"Price":21.67,
		"Quantity":1
	},
	{
		"Product Name":"Bowler Hat",
		"ProductID":858383,
		"SKU":"040657863",
		"Description":{
			"Colour":"Purple",
			"Width":300,
			"Height":200,
			"Depth":210,
			"Weight":0.75
		},
		"Price":34.45,
		"Quantity":4
	},
	{
		"ProductID":345664,
		"SKU":"0406654603",
		"Product Name":"Cloak",
		"Description":{
			"Colour":"Black",
			"Width":30,
			"Height":20,
			"Depth":210,
			"Weight":2
		},
		"Price":107.99,
		"Quantity":1
	}
]
````

* Query: Account.Order.Product."Product Name"
* Result: 
````json
["Bowler Hat","Trilby hat","Bowler Hat","Cloak"]
````

### Expand arrays
The previous examples with the option expand set to true would provide the same results but in 4 records of a single object in the first example or simple varchar in the second one.

### serialize objects
In the case of the first example if we want to map specfic fields from this result set, we need to set the option serialize to true (and expand to true as well since the result set in an array) and specifiy output fields:
* ProductID: will be automatically mapped to the field ProductID
* ProductName: if we want to the map the field Product Name, DataStage does not like spaces in fields names so we have to specify the mapping in the derivation attribute: /Product Name
* Colour: this field is a nested Object that can be mapped through de derivation specification: /Description/Colour

![serialize](https://res.cloudinary.com/dmeujvgly/image/upload/v1613992276/jsonata2.png)

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.



## License
[MIT](https://choosealicense.com/licenses/mit/)


