#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from typing import Dict, List

from pymongo import MongoClient, ReturnDocument

from ..institution_extracters import constants as institution_extracter_constants
from ..institution_extracters import exceptions
from ..institution_extracters.google_sheets_institution_extracter import (
    FormattedSheetData,
)
from . import constants as database_constants

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

    def _find_one_and_update(
        self, collection: str, primary_keys: Dict[str, str], document: Dict[str, str]
    ) -> Dict[str, str]:
        """
        Insert/update a document into the collection.

        Parameters
        ----------
        collection: str
            The the collection the document should be added to.
        primary_keys: Dict[str, str]
            The primary keys and their values to specify a unique document in the collection.
        document: Dict[str, str]
            The document to be inserted/updated.

        Returns
        ----------
        document: Dict[str, str]
            The inserted/updated document.
        """
        document = self._db.get_collection(collection).find_one_and_update(
            primary_keys,
            {"$set": document},
            return_document=ReturnDocument.AFTER,
            upsert=True,
        )
        log.debug(f"Uploaded {str(document)} to collection {collection}")
        return document

    def _load_institutions(
        self,
        institution_primary_keys: List[str],
        variable_primary_keys: List[str],
        formatted_data: List,
    ) -> List[Dict]:
        document_ids = []
        for institution in formatted_data:
            institution_without_variables_field = {**institution}
            institution_without_variables_field.pop(
                database_constants.DatabaseCollection.variables
            )
            institution_doc = self._find_one_and_update(
                database_constants.DatabaseCollection.institutions,
                {pk: institution.get(pk) for pk in institution_primary_keys},
                institution_without_variables_field,
            )
            document_ids.append(
                {
                    "_id": institution_doc.get("_id"),
                    "collection": database_constants.DatabaseCollection.institutions,
                }
            )
            for variable in institution.get(
                database_constants.DatabaseCollection.variables
            ):
                variable_pks = {pk: variable.get(pk) for pk in variable_primary_keys}
                variable_pks["institution_id"] = institution_doc.get("_id")
                variable_doc = self._find_one_and_update(
                    database_constants.DatabaseCollection.variables,
                    variable_pks,
                    variable,
                )
                document_ids.append(
                    {
                        "_id": variable_doc.get("_id"),
                        "collection": database_constants.DatabaseCollection.variables,
                    }
                )
            log.info(
                f"Finished loading institution {str({pk: institution.get(pk) for pk in institution_primary_keys})}"
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
            The list of document ids and their collections.
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
            log.info(
                f"Found and loaded {len(document_ids_by_collection.get(collection))} {collection}."
            )
            delete_result = self._db.get_collection(collection).delete_many(
                {"_id": {"$nin": document_ids_by_collection.get(collection)}}
            )
            log.info(
                f"Deleted {delete_result.deleted_count} old documents from {collection}."
            )

    def load(self, formatted_sheet_data: FormattedSheetData) -> List[Dict]:
        """
        Load the formatted sheet data into the database.

        Parameters
        ----------
        formatted_sheet_data: FormattedSheetData
            The formatted sheet data. Please see the class FormattedSheetData to view its attributes.

        Returns
        ----------
        document_ids: List[A1Notatoin]
            The list of inserted/updated document ids and their collections.
        """
        google_sheets_format = formatted_sheet_data.meta_data.get("format")
        if (
            google_sheets_format
            == institution_extracter_constants.GoogleSheetsFormat.institution_by_triples
        ):
            return self._load_institutions(
                ["name", "country", "category"],
                ["heading", "name"],
                formatted_sheet_data.formatted_data,
            )
            log.info(f"Finished loading sheet {formatted_sheet_data.sheet_title}")
        elif (
            google_sheets_format
            == institution_extracter_constants.GoogleSheetsFormat.institution_by_rows
        ):
            return self._load_institutions(
                ["name", "category"],
                ["heading", "name"],
                formatted_sheet_data.formatted_data,
            )
            log.info(f"Finished loading sheet {formatted_sheet_data.sheet_title}")
        else:
            raise exceptions.UnrecognizedGoogleSheetsFormat(
                formatted_sheet_data.sheet_title, google_sheets_format
            )

    def __str__(self):
        return f"<MongoDBDatabase [{self._db_connection_url}]>"

    def __repr__(self):
        return str(self)
