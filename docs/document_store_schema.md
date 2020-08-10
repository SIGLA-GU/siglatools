# Document Store Schema

## Institutions

_Schema_
```
institution_id: {
    name: str
    category: str
    sub_category: Optional[str]
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
    institution_id: str
    heading: str
    name: str
    variable_index: int
    sigla_answer_index: int
    sigla_answer: str || List[List[Dict]]
    orig_text: Optional[str]
    source: Optional[str]
    type: "standard" || "composite" || "aggregate"
    hyperlink: Optional["rights" || "amendments" || "legal_framework"]
}
```

_Notes_

`institution_id` is used to capture a many-to-one relationship with an institution.
`type` specifies the variable type. standard variable has the sigla triple: `sigla_answer`, `orig_text`, `source` as str, with `orig_text` being optional for International Institutions. composite variable also has a sigla triple, but has an additional `hyperlink` field to link to another collection. For example, the Constitutional Rights variable will have a hyperlink to the rights collection. aggregate variable doesn't have a sigla triple, since it is missing `orig_text` and `source`. Instead its `sigla_answer` is a list of list of dict.


## Rights

_Schema_
```
right_id: {
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
