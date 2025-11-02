from __future__ import annotations

from sqlacodegen.generators import SQLModelGenerator
from sqlalchemy import ForeignKeyConstraint
from sqlalchemy.sql.schema import Column

class ForeignKeyIdSuffixGenerator(SQLModelGenerator):
    """
    - If the column is a FK of a single column to <table>.id and its name matches
      the destination table (singularized if 'use_inflect') and does not end in '_id',
      rename the column attribute to '<name>_id'.
    - The relationship will automatically be named '<name>' thanks to the base logic.
    """

    def generate_column_attr_name(
        self,
        column_attr,
        global_names: set[str],
        local_names: set[str],
    ) -> None:
        col: Column = column_attr.column
        preferred = col.name

        single_fk = None
        if col.foreign_keys:
            # Search for dedicated FK (constraint of a single column)
            for fk in col.foreign_keys:
                if fk.constraint and isinstance(fk.constraint, ForeignKeyConstraint):
                    if len(fk.constraint.columns) == 1:
                        single_fk = fk
                        break

        if single_fk:
            target_table = single_fk.column.table.name
            target_singular = target_table
            if "use_inflect" in self.options:
                singular = self.inflect_engine.singular_noun(target_table)
                if singular:
                    target_singular = singular

            # If column name is the same as the destination table (singular) and does not end in _id,
            # rename it to *_id to free the base name for the relationship.
            if preferred == target_singular and not preferred.endswith("_id"):
                preferred = f"{preferred}_id"

        column_attr.name = self.find_free_name(preferred, global_names, local_names)
