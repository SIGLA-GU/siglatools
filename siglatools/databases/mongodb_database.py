#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from typing import Dict, List

from pymongo import MongoClient, ReturnDocument

from ..institution_extracters import exceptions
from ..institution_extracters.constants import GoogleSheetsFormat as gs_format
from ..institution_extracters.utils import FormattedSheetData
from .constants import DatabaseCollection as db_collection

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
            gs_format.institution_by_triples: self._load_institutions,
            gs_format.institution_by_rows: self._load_institutions,
            gs_format.institution_and_composite_variable: self._load_composite_variable,
            gs_format.composite_variable: self._load_composite_variable,
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
        ----------
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
        ----------
        document: Dict[str, str]
            The inserted document or replacement document.
        """
        document = self._db.get_collection(collection).find_one_and_replace(
            primary_keys, document, return_document=ReturnDocument.AFTER, upsert=True,
        )
        log.debug(f"Uploaded {str(document)} to collection {collection}")
        return document

    def _load_composite_variable(
        self, formatted_sheet_data: FormattedSheetData
    ) -> List[Dict]:
        """
        Load composite variable into the database.

        Parameters
        ----------
        formatted_sheet_data: FormattedSheetData
            The data to be loaded into the database. Please see the FormattedSheetData class to view its attributes.

        Returns
        ----------
        document_ids: List[Dict]
            The list of loaded document ids and their collections.
        """
        document_ids = []
        # Create the institution primary keys
        institution = {
            "name": formatted_sheet_data.meta_data.get("name"),
            "country": formatted_sheet_data.meta_data.get("country"),
            "category": formatted_sheet_data.meta_data.get("category"),
        }
        # Find the institution where the composite variable resides
        institution_doc = self._find_one(db_collection.institutions, institution,)
        # Add the institution doc id to the list of loaded doc ids
        document_ids.append(
            {
                "_id": institution_doc.get("_id"),
                "collection": db_collection.institutions,
            }
        )
        # Create the composite variable primary keys
        variable = {
            "institution_id": institution_doc.get("_id"),
            "heading": formatted_sheet_data.meta_data.get("variable_heading"),
            "name": formatted_sheet_data.meta_data.get("variable_name"),
        }
        # Find the composite variable
        variable_doc = self._find_one(db_collection.variables, variable)
        if (
            formatted_sheet_data.meta_data.get("format")
            == gs_format.institution_and_composite_variable
        ):
            # Create a more specific instiution that will be associated with only the composite variable
            specific_institution = {
                "name": formatted_sheet_data.meta_data.get("variable_heading"),
                "country": formatted_sheet_data.meta_data.get("country"),
                "category": formatted_sheet_data.meta_data.get("category"),
                "child_collection": formatted_sheet_data.meta_data.get("data_type")
            }
            # Find the specific institution
            specific_institution_doc = self._find_one(
                db_collection.institutions, specific_institution
            )
            # Add the specific institution doc id to the list of loaded doc ids
            document_ids.append(
                {
                    "_id": specific_institution_doc.get("_id"),
                    "collection": db_collection.institutions,
                }
            )
        # Associate each datum with the doc id of the found composite variable
        # and load it in to the database
        for datum in formatted_sheet_data.formatted_data:
            # Associate the datum with the variable doc id
            datum["variable_id"] = variable_doc.get("_id")
            if (
                formatted_sheet_data.meta_data.get("format")
                == gs_format.institution_and_composite_variable
            ):
                # Associate the datum with the specific institution doc id
                datum["institution_id"] = specific_institution_doc.get("_id")
            # Load the datum into the database
            datum_doc = self._find_one_and_replace(
                formatted_sheet_data.meta_data.get("data_type"),
                {"variable_id": datum.get("variable_id"), "index": datum.get("index")},
                datum,
            )
            # Add the datum doc id to the list of loaded doc ids
            document_ids.append(
                {
                    "_id": datum_doc.get("_id"),
                    "collection": formatted_sheet_data.meta_data.get("data_type"),
                }
            )
        return document_ids

    def _load_institutions(
        self, formatted_sheet_data: FormattedSheetData,
    ) -> List[Dict]:
        """
        Load institutions and their variables in to the database.

        Parameters
        ----------
        formatted_sheet_data: FormattedSheetData
            The data to be loaded into the database. Please see the FormattedSheetData class to view its attributes.

        Returns
        ----------
        document_ids: List[Dict]
            The list of loaded document ids and their collections.
        """
        document_ids = []

        # Iterate over the list of institutions
        for institution in formatted_sheet_data.formatted_data:
            # Create institution filter
            institution_filter = {
                "name": institution.get("name"),
                "category": institution.get("category"),
            }
            # Add country to the institution filter if the format is instiution-by-triples
            if "country" in formatted_sheet_data.meta_data:
                institution_filter["country"] = institution.get("country")
            # Copy the institution without the child field
            institution_without_childs_field = {**institution}
            institution_without_childs_field.pop("childs")
            # Find the institution
            institution_doc = self._find_one_and_replace(
                db_collection.institutions,
                institution_filter,
                institution_without_childs_field,
            )
            # Add the institution doc id to the list of loaded doc ids
            document_ids.append(
                {
                    "_id": institution_doc.get("_id"),
                    "collection": db_collection.institutions,
                }
            )
            # Iterate over the list of childs
            for child in institution.get("childs"):
                # Create the child filter
                child_filter = {
                    "institution_id": institution_doc.get("_id"),
                    "heading": child.get("heading"),
                    "name:": child.get("name"),
                }
                # Find the child
                child_doc = self._find_one_and_replace(
                    formatted_sheet_data.meta_data.get("data_type"),
                    child_filter,
                    {**child, "institution_id": institution_doc.get("_id")},
                )
                # Add the variable doc id to the list of loaded doc ids
                document_ids.append(
                    {"_id": child_doc.get("_id"), "collection": db_collection.variables}
                )

            log.info(
                f"Finished loading the {db_collection.variables} of institution "
                f"{str(institution_filter)}"
            )
        return document_ids

    def close_connection(self):
        """
        Cleanup client resources and disconnect from MongoDB.
        """
        self._client.close()

    def clean_up(self, document_ids: List[Dict]):
        """
        Delete documents from the database that are not in the list of document_ids.

        Parameters
        ----------
        document_ids: List[Dict]
            The list of inserted/updated document ids and their collections.
        """

        # Create a list of document ids for each collection.
        document_ids_by_collection = {}
        for document in document_ids:
            collection = document.get("collection")
            doc_id = document.get("_id")
            if collection in document_ids_by_collection:
                document_ids_by_collection.get(collection).append(doc_id)
            else:
                document_ids_by_collection[collection] = [doc_id]

        #  Delete documents from collection if they are not in the list of document ids.
        for collection in document_ids_by_collection.keys():
            unique_document_ids = set(document_ids_by_collection.get(collection))
            log.info(f"Found and loaded {len(unique_document_ids)} {collection}.")
            delete_result = self._db.get_collection(collection).delete_many(
                {"_id": {"$nin": document_ids_by_collection.get(collection)}}
            )
            log.info(
                f"Deleted {delete_result.deleted_count} old documents from {collection}."
            )

    def load(self, formatted_sheet_data: FormattedSheetData) -> List[Dict[str, str]]:
        """
        Load the formatted sheet data into the database.

        Parameters
        ----------
        formatted_sheet_data: FormattedSheetData
            The formatted sheet data. Please see the class FormattedSheetData to view its attributes.

        Returns
        ----------
        document_ids: List[Dict[str, str]]
            The list of inserted/updated document ids and their collections.
        """
        load_function_key = formatted_sheet_data.meta_data.get("format")
        if load_function_key in self._load_function_dict:
            return self._load_function_dict[load_function_key](formatted_sheet_data)
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
