#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from typing import Any, Dict, List, Optional, Tuple

from bson.objectid import ObjectId
from pymongo import DeleteMany, MongoClient, ReturnDocument, UpdateOne, UpdateMany

from ..institution_extracters import exceptions
from ..institution_extracters.constants import GoogleSheetsFormat as gs_format
from ..institution_extracters.utils import FormattedSheetData
from .constants import DatabaseCollection as db_collection, InstitutionField
from .constants import VariableType
from .exceptions import UnableToFindDocument

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
            gs_format.institution_and_composite_variable: self._load_institution_and_composite_variable,
            gs_format.composite_variable: self._load_composite_variable,
            gs_format.multiple_sigla_answer_variable: self._load_institutions,
        }

    def _create_variable_reference(self, sheet_title: str, meta_data: Dict[str, str]):
        institution_names = [name.strip() for name in meta_data.get(InstitutionField.name).split(";")]

        institution = {
            InstitutionField.name: {"$in": institution_names},
            InstitutionField.country: meta_data.get(InstitutionField.country),
            InstitutionField.category: meta_data.get(InstitutionField.category),
        }
        institution_docs = self.find(db_collection.institutions, institution)
        institution_docs_id = [doc.get(InstitutionField._id) for doc in institution_docs]

        variable = {
            "institution": {"$in": institution_docs_id},
            "heading": meta_data.get("variable_heading"),
            "name": meta_data.get("variable_name"),
        }
        variable_docs = self.find(db_collection.variables, variable)
        variable_docs_id = [doc.get("_id") for doc in variable_docs]

        if len(variable_docs_id) != len(institution_names):
            raise UnableToFindDocument(sheet_title, db_collection.variables, variable)

        # update the variables to have type composite and the right hyperlink
        update_variables_request = UpdateMany(
            {"_id": {"$in": variable_docs_id}},
            {
                "$set": {
                    "type": VariableType.composite,
                    "hyperlink": meta_data.get("data_type"),
                }
            },
        )
        update_variables_request_result = self._db.get_collection(
            db_collection.variables
        ).bulk_write([update_variables_request])
        log.info(
            f"Update {update_variables_request_result.modified_count}/{len(variable_docs_id)} variables"
        )

        if meta_data.get("data_type") == db_collection.body_of_law:
            return {"variables": variable_docs_id}
        else:
            return {"variable": variable_docs_id[0]}

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
        document = self._db.get_collection(collection).find_one_and_update(
            primary_keys,
            {"$set": primary_keys},
            return_document=ReturnDocument.AFTER,
            upsert=True,
        )
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
            InstitutionField.spreadsheet_id: formatted_sheet_data.spreadsheet_id,
            InstitutionField.sheet_id: formatted_sheet_data.sheet_id,
            InstitutionField.name: formatted_sheet_data.meta_data.get("variable_heading"),
            InstitutionField.country: formatted_sheet_data.meta_data.get(InstitutionField.country),
            InstitutionField.category: formatted_sheet_data.meta_data.get(InstitutionField.category),
        }
        # Find the specific institution
        institution_doc = self._find_one(db_collection.institutions, institution)
        log.info(
            f"Loaded 1 {db_collection.institutions} "
            f"from sheet: {formatted_sheet_data.sheet_title}"
        )
        # Create a dict with variable heading(category of rights) as keys and the list of rights as the values
        variable_heading_dict = {}
        variable_heading_list = []
        for datum in formatted_sheet_data.formatted_data:
            # Category of a right is the first element in sigla_answers field of datum
            variable_heading = datum.get("sigla_answers")[0].get("answer")
            sigla_answers = datum.get("sigla_answers")[1:]
            if variable_heading in variable_heading_dict:
                variable_heading_dict.get(variable_heading).append(sigla_answers)
            else:
                variable_heading_list.append(variable_heading)
                variable_heading_dict[variable_heading] = [sigla_answers]
        # Create the list of variables
        variables = [
            {
                "institution": institution_doc.get(InstitutionField._id),
                "name": variable_heading,
                "heading": variable_heading,
                "sigla_answer": variable_heading_dict.get(variable_heading),
                "type": VariableType.aggregate,
                "variable_index": i,
            }
            for i, variable_heading in enumerate(variable_heading_list)
        ]
        # Create the list of update requests into the db, one for each variable
        update_requests = [
            UpdateOne(
                {
                    "institution": variable.get("institution"),
                    "name": variable.get("name"),
                    "variable_index": variable.get("variable_index"),
                },
                {"$set": variable},
                upsert=True,
            )
            for i, variable in enumerate(variables)
        ]
        # Bulk write the variables into the db
        update_requests_results = self._db.get_collection(
            db_collection.variables
        ).bulk_write(update_requests)

        log.info(
            f"Loaded {update_requests_results.upserted_count} {db_collection.variables} "
            f"from sheet: {formatted_sheet_data.sheet_title}"
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
        # Get the composite variable reference
        variable_reference = self._create_variable_reference(
            formatted_sheet_data.sheet_title, formatted_sheet_data.meta_data
        )
        # Create the list of update requests into the db, one for each row of the composite variable
        update_requests = [
            UpdateOne(
                {**variable_reference, "index": datum.get("index")},
                {"$set": {**variable_reference, **datum}},
                upsert=True,
            )
            for datum in formatted_sheet_data.formatted_data
        ]
        # Bulk write the composite variable into the db
        update_requests_results = self._db.get_collection(data_type).bulk_write(
            update_requests
        )
        log.info(
            f"Loaded {update_requests_results.upserted_count} {data_type} "
            f"from sheet: {formatted_sheet_data.sheet_title}"
        )

    def _load_institutions(
        self,
        formatted_sheet_data: FormattedSheetData,
    ):
        """
        Load institutions and their variables in to the database.

        Parameters
        ----------
        formatted_sheet_data: FormattedSheetData
            The data to be loaded into the database. Please see the FormattedSheetData class to view its attributes.
        """
        institution_primary_keys = [InstitutionField.name, InstitutionField.category]
        if InstitutionField.country in formatted_sheet_data.meta_data:
            institution_primary_keys.append(InstitutionField.country)
        # Create the list of update requests into the db, one for each institution
        institution_requests = [
            UpdateOne(
                {pk: institution.get(pk) for pk in institution_primary_keys},
                {
                    "$set": {
                        key: institution.get(key)
                        for key in institution.keys()
                        if key != "childs"
                    }
                },
                upsert=True,
            )
            for institution in formatted_sheet_data.formatted_data
        ]
        # Bulk write the institutions in the db
        institution_requests_results = self._db.get_collection(
            db_collection.institutions
        ).bulk_write(institution_requests)
        log.info(
            f"Loaded {institution_requests_results.upserted_count} {db_collection.institutions} "
            f"from sheet: {formatted_sheet_data.sheet_title}"
        )
        # Get doc id for each institution
        institution_doc_id_dict = {}
        for i, institution in enumerate(formatted_sheet_data.formatted_data):
            upserted_id = institution_requests_results.upserted_ids.get(i)
            if upserted_id is None:
                # The institution wasn't upserted
                # Find the doc
                institution_doc = self._db.get_collection(
                    db_collection.institutions
                ).find_one({pk: institution.get(pk) for pk in institution_primary_keys})
                institution_doc_id_dict[i] = institution_doc.get(InstitutionField._id)
            else:
                institution_doc_id_dict[i] = upserted_id

        # Create the list of update requests into the db, one for each variable
        variable_requests = [
            UpdateOne(
                {
                    "institution": institution_doc_id_dict.get(i),
                    "heading": child.get("heading"),
                    "name": child.get("name"),
                    "variable_index": child.get("variable_index"),
                },
                {"$set": {"institution": institution_doc_id_dict.get(i), **child}},
                upsert=True,
            )
            for i, institution in enumerate(formatted_sheet_data.formatted_data)
            for child in institution.get("childs")
        ]
        # Bulk write the variables in the db
        variable_requests_results = self._db.get_collection(
            db_collection.variables
        ).bulk_write(variable_requests)
        log.info(
            f"Loaded {variable_requests_results.upserted_count} {db_collection.variables} "
            f"from sheet: {formatted_sheet_data.sheet_title}"
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

    def find(
        self,
        collection: str,
        filters: Dict[str, Any],
        sort: Optional[List[Tuple[str, str]]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Query the database for documents.

        Parameters
        ----------
        collection: str
            The db collection to query for documents.
        filters: Dict[str, Any]
            A prototype document that all results must match.
        sort: Optional[List[Tuple[str, str]]]
            A list of (key, direction) pairs specifying the sort order for this query.

        Returns
        -------
        docs: List[Dict[str, Any]]
            The list of matched documents.
        """
        cursor = self._db.get_collection(collection).find(filters)
        if sort:
            cursor.sort(sort)
        return [doc for doc in cursor]

    def delete_many(self, collection: str, doc_ids: List[ObjectId]):
        """
        Delete documents from the database.

        Parameters
        ----------
        collection: str
            The db collection to delete document from.
        doc_ids: List[ObjectId]
            The list of document ids to delete.
        """
        delete_request = DeleteMany({"_id": {"$in": doc_ids}})
        delete_many_results = self._db.get_collection(collection).bulk_write(
            [delete_request]
        )

        delete_msg = (
            f"Deleted {delete_many_results.deleted_count}/{len(doc_ids)} {collection}."
        )
        if delete_many_results.deleted_count != len(doc_ids):
            log.error(delete_msg)
        else:
            log.info(delete_msg)

    def __str__(self):
        return f"<MongoDBDatabase [{self._db_connection_url}]>"

    def __repr__(self):
        return str(self)
