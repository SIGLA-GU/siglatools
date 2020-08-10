#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from typing import Dict

from pymongo import MongoClient, ReturnDocument, UpdateOne

from ..institution_extracters import exceptions
from ..institution_extracters.constants import GoogleSheetsFormat as gs_format
from ..institution_extracters.utils import FormattedSheetData
from .constants import DatabaseCollection as db_collection
from .constants import VariableType

###############################################################################

logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)4s: %(module)s:%(lineno)4s %(asctime)s] %(message)s",
)
log = logging.getLogger(__name__)

###############################################################################


class MongoDBDatabase:
    def __init__(self, db_connection_url: str):
        self._client = MongoClient(db_connection_url, connect=False)
        self._db_connection_url = db_connection_url
        self._db = self._client.get_default_database()
        self._load_function_dict = {
            gs_format.standard_institution: self._load_institutions,
            gs_format.institution_by_rows: self._load_institutions,
            gs_format.institution_and_composite_variable: self._load_institution_and_composite_variable,
            gs_format.composite_variable: self._load_composite_variable,
            gs_format.multiple_sigla_answer_variable: self._load_institutions,
        }

    def _find_one(
        self, collection: str, primary_keys: Dict[str, str]
    ) -> Dict[str, str]:
        """
        Find a document in given collection with the given primary keys.
        If it doesn't exist, insert the document into the database.

        Parameters
        ----------
        collection: str
            The the collection the document should be added to.
        primary_keys: Dict[str, str]
            The primary keys and their values to specify a unique document in the collection.

        Returns
        -------
        document: Dict[str, str]
            The found/inserted document.

        """
        # Find the document
        db_document = self._db.get_collection(collection).find_one_and_update(
            primary_keys,
            {"$set": primary_keys},
            return_document=ReturnDocument.AFTER,
            upsert=True,
        )
        return db_document

    def _find_one_and_replace(
        self, collection: str, primary_keys: Dict[str, str], document: Dict[str, str]
    ) -> Dict[str, str]:
        """
        Insert a document into the collection. If the document already exist, replace it with the new document.

        Parameters
        ----------
        collection: str
            The the collection the document should be added to.
        primary_keys: Dict[str, str]
            The primary keys and their values to specify a unique document in the collection.
        document: Dict[str, str]
            The inserted document or replacement document.

        Returns
        -------
        document: Dict[str, str]
            The inserted document or replacement document.
        """
        document = self._db.get_collection(collection).find_one_and_replace(
            primary_keys, document, return_document=ReturnDocument.AFTER, upsert=True,
        )
        log.debug(f"Uploaded {str(document)} to collection {collection}")
        return document

    def _load_institution_and_composite_variable(
        self, formatted_sheet_data: FormattedSheetData
    ):
        """
        Load the special institution that is also a composite variable in to the db.
        Load the composite variable into the db.

        Parameters
        ----------
        formatted_sheet_data: FormattedSheetData
            The data to be loaded into the database. Please see the FormattedSheetData class to view its attributes.
        """
        self._load_composite_variable(formatted_sheet_data)
        self._load_institution_with_aggregate_variable(formatted_sheet_data)

    def _load_institution_with_aggregate_variable(
        self, formatted_sheet_data: FormattedSheetData
    ):
        """
        Load the special institution that is also a composite variable in to the db.

        Parameters
        ----------
        formatted_sheet_data: FormattedSheetData
            The data to be loaded into the database. Please see the FormattedSheetData class to view its attributes.
        """
        # Create the institution primary keys
        institution = {
            "name": formatted_sheet_data.meta_data.get("variable_heading"),
            "country": formatted_sheet_data.meta_data.get("country"),
            "category": formatted_sheet_data.meta_data.get("category"),
        }
        # Find the specific institution
        institution_doc = self._find_one(db_collection.institutions, institution)

        # Create a dict with variable heading(category of rights) as keys and the list of rights as the values
        variable_heading_dict = {}
        variable_heading_list = []
        for datum in formatted_sheet_data.formatted_data:
            # Category of a right is the first element in sigla_answers field of datum
            variable_heading = datum.get("sigla_answers")[0].get("name")
            sigla_answers = datum.get("sigla_answers")[1:]
            if variable_heading in variable_heading_dict:
                variable_heading_dict.get(variable_heading).append(sigla_answers)
            else:
                variable_heading_list.append(variable_heading)
                variable_heading_dict[variable_heading] = [sigla_answers]
        # Create the list of variables
        variables = [
            {
                "institution_id": institution_doc.get("_id"),
                "name": variable_heading,
                "heading": variable_heading,
                "sigla_answer": variable_heading_dict.get(variable_heading),
                "type": VariableType.aggregate,
                "variable_index": i,
                "sigla_answer_index": 0,
            }
            for i, variable_heading in enumerate(variable_heading_list)
        ]
        # Create the list of update requests into the db, one for each variable
        update_requests = [
            UpdateOne(
                {
                    "instiution_id": variable.get("institution_id"),
                    "name": variable.get("name"),
                    "variable_index": variable.get("variable_index"),
                    "sigla_answer_index": variable.get("sigla_answer_index"),
                },
                variable,
                upsert=True,
            )
            for i, variable in enumerate(variables)
        ]
        # Bulk write the variables into the db
        update_requests_results = self._db.get_collection(
            db_collection.variables
        ).bulk_write(update_requests)

        log.info(
            f"Loaded {len(update_requests_results.upserted_ids.keys())} {db_collection.variables}"
        )

    def _load_composite_variable(self, formatted_sheet_data: FormattedSheetData):
        """
        Load composite variable into the database.

        Parameters
        ----------
        formatted_sheet_data: FormattedSheetData
            The data to be loaded into the database. Please see the FormattedSheetData class to view its attributes.
        """
        data_type = formatted_sheet_data.meta_data.get("data_type")
        # Create the institution primary keys
        institution = {
            "name": formatted_sheet_data.meta_data.get("name"),
            "country": formatted_sheet_data.meta_data.get("country"),
            "category": formatted_sheet_data.meta_data.get("category"),
        }
        # Find the institution where the composite variable resides
        institution_doc = self._find_one(db_collection.institutions, institution,)

        # Create the composite variable primary keys
        variable = {
            "institution_id": institution_doc.get("_id"),
            "heading": formatted_sheet_data.meta_data.get("variable_heading"),
            "name": formatted_sheet_data.meta_data.get("variable_name"),
        }
        # Find the composite variable
        variable_doc = self._find_one(db_collection.variables, variable)
        # Create the list of update requests into the db, one for each row of the composite variable
        update_requests = [
            UpdateOne(
                {"variable_id": variable_doc.get("_id"), "index": datum.get("index")},
                {"variable_id": variable_doc.get("_id"), **datum},
                upsert=True,
            )
            for datum in formatted_sheet_data.formatted_data
        ]
        # Bulk write the composite variable into the db
        update_requests_results = self._db.get_collection(data_type).bulk_write(
            update_requests
        )

        log.info(
            f"Loaded {len(update_requests_results.upserted_ids.keys())} {data_type}"
        )

    def _load_institutions(
        self, formatted_sheet_data: FormattedSheetData,
    ):
        """
        Load institutions and their variables in to the database.

        Parameters
        ----------
        formatted_sheet_data: FormattedSheetData
            The data to be loaded into the database. Please see the FormattedSheetData class to view its attributes.
        """
        institution_primary_keys = ["name", "category"]
        if (
            formatted_sheet_data.meta_data.get("format")
            == gs_format.standard_institution
        ):
            institution_primary_keys.append("country")
        # Create the list of update requests into the db, one for each institution
        institution_requests = [
            UpdateOne(
                {pk: institution.get(pk) for pk in institution_primary_keys},
                {
                    key: institution.get(key)
                    for key in institution.keys()
                    if key != "childs"
                },
                upsert=True,
            )
            for institution in formatted_sheet_data.formatted_data
        ]
        # Bulk write the institutions in the db
        institution_requests_results = self._db.get_collection(
            db_collection.institutions
        ).bulk_write(institution_requests)
        # Create the list of update requests into the db, one for each variable
        variable_reqquests = [
            UpdateOne(
                {
                    "institution_id": institution_requests_results.upserted_ids.get(i),
                    "heading": child.get("heading"),
                    "name": child.get("name"),
                    "variable_index": child.get("variable_index"),
                    "sigla_answer_index": child.get("sigla_answer_index"),
                },
                {
                    "institution_id": institution_requests_results.upserted_ids.get(i),
                    **child,
                },
                upsert=True,
            )
            for i, institution in enumerate(formatted_sheet_data.formatted_data)
            for child in institution.get("childs")
        ]
        # Bulk write the variables in the db
        variable_requests_results = self._db.get_collection(
            db_collection.variables
        ).bulk_write(variable_reqquests)

        log.info(
            f"Loaded {len(institution_requests_results.upserted_ids.keys())} {db_collection.institutions}"
        )
        log.info(
            f"Loaded {len(variable_requests_results.upserted_ids.keys())} {db_collection.variables}"
        )

    def close_connection(self):
        """
        Cleanup client resources and disconnect from MongoDB.
        """
        self._client.close()

    def clean_up(self):
        """
        Delete all documents from the database.

        """

        for collection in self._db.list_collection_names():
            delete_result = self._db.get_collection(collection).delete_many({})
            log.info(
                f"Deleted {delete_result.deleted_count} old documents from {collection}."
            )

    def load(self, formatted_sheet_data: FormattedSheetData):
        """
        Load the formatted sheet data into the database.

        Parameters
        ----------
        formatted_sheet_data: FormattedSheetData
            The formatted sheet data. Please see the class FormattedSheetData to view its attributes.
        """
        load_function_key = formatted_sheet_data.meta_data.get("format")
        if load_function_key in self._load_function_dict:
            self._load_function_dict[load_function_key](formatted_sheet_data)
        else:
            raise exceptions.UnrecognizedGoogleSheetsFormat(
                formatted_sheet_data.sheet_title,
                load_function_key,
                formatted_sheet_data.meta_data.get("data_type"),
            )

    def __str__(self):
        return f"<MongoDBDatabase [{self._db_connection_url}]>"

    def __repr__(self):
        return str(self)
