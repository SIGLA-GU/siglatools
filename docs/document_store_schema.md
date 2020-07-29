# Document Store Schema

## Institutions

_Schema_
```
institution_id: {
    name: str
    category: str
    sub_category: optional[str]
    country: optional[str]
    child_collection: str
}
```

_Notes_

`child_collection` is used specifiy which collection the institution has a one-to-many relation with.
`country` is optional for International Institutions.
`sub_category` is for institutions that has another level of categorization.


## Variables

_Schema_
```
variable_id: {
    institution_id: str
    heading: str
    name: str
    index: int
    sigla_answer: str
    orig_text: optional[str]
    source: str
    type: "standard" || "composite"
    hyperlink: optional["rights" || "amendments" || "legal_framework"]
}
```

_Notes_

`variable_id` is used to capture a many-to-one relationship with an institution.
`orig_text` is optional for International Institutions' variables.
`type` specify whether the variable will link to another collection. standard variables don't link to another collection (thus,`hyperlink` will be ommitted), while composite variable will have a `hyperlink`. For example, the Constitutional Rights variable will
have a hyperlink to the rights collection.


## Rights

_Schema_
```
right_id: {
    institution_id: str
    variable_id: str
    index: int
    sigla_answers: [
        {
            name: str
            sigla_answer: str
        }
    ]
}
```

_Notes_

`variable_id` is used to capture a many-to-one relationship with the Constitutional Rights variable.
`institution_id` is used to capture a many-to-one-relationship with the special Constitutional Rights institution.


## Amendments

_Schema_
```
amendment_id: {
    variable_id: str
    index: int
    sigla_answers: [
        {
            name: str
            sigla_answer: str
        }
    ]
}
```

_Notes_

`variable_id` is used to capture a many-to-one relationship with the Constitutional Amendments variable.

## Legal Framework

_Schema_
```
legal_framework_id: {
    variable_id: str
    index: int
    sigla_answers: [
        {
            name: str
            sigla_answer: str
        }
    ]
}
```

_Notes_

`variable_id` is used to capture a many-to-one relationship with the Legal Framework variable.
