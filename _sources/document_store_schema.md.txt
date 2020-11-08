# Document Store Schema

## Institutions

_Schema_
```
institution_id: {
    name: str
    category: str
    sub_category: Optional[List[str]]
    country: Optional[str]
}
```

_Notes_

`country` is optional for International Institutions.
`sub_category` is for institutions that has another level of categorization.


## Variables

_Schema_
```
variable_id: {
    institution: {
        type: ObjectId,
        ref: institutions._id
    }
    heading: str
    name: str
    variable_index: int
    sigla_answer: str || List[List[Dict]]
    orig_text: Optional[str]
    source: Optional[str]
    type: "standard" || "composite" || "aggregate"
    hyperlink: Optional["rights" || "amendments" || "legal_framework"]
}
```

_Notes_

`institution` is used to capture a many-to-one relationship with an institution.
`variable_index` specifies the order of the variable.
`type` specifies the variable type. standard variable has the sigla triple: `sigla_answer`, `orig_text`, `source` as str, with `orig_text` being optional for International Institutions. composite variable also has a sigla triple, but has an additional `hyperlink` field to link to another collection. For example, the Constitutional Rights variable will have a hyperlink to the rights collection. aggregate variable doesn't have a sigla triple, since it is missing `orig_text` and `source`. Instead its `sigla_answer` is a list of list of dict with the format:
```
sigla_answer: [
    [
        {
            name: str
            answer: str
        },
    ],
]
```


## Rights

_Schema_
```
right_id: {
    variable: {
        type: ObjectId,
        ref: variables._id
    }
    index: int
    sigla_answers: [
        {
            name: str
            answer: str
        }
    ]
}
```

_Notes_

`variable` is used to capture a many-to-one relationship with the Constitutional Rights variable.


## Amendments

_Schema_
```
amendment_id: {
    variable: {
        type: ObjectId,
        ref: variables._id
    }
    index: int
    sigla_answers: [
        {
            name: str
            answer: str
        }
    ]
}
```

_Notes_

`variable` is used to capture a many-to-one relationship with the Constitutional Amendments variable.

## Legal Framework

_Schema_
```
legal_framework_id: {
    variables: [
        {
            type: ObjectId,
            ref: variables._id
        },
    ]
    index: int
    sigla_answers: [
        {
            name: str
            answer: str
        }
    ]
}
```

_Notes_

`variables` is used to capture a many-to-many relationship with the Legal Framework variable. A legal framework(in total) can be a legal framework for many variables.
