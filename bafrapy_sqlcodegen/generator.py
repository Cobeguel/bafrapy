from __future__ import annotations

from collections import defaultdict

from sqlacodegen.generators import DeclarativeGenerator, SQLModelGenerator
from sqlacodegen.models import ColumnAttribute, Model, ModelClass
from sqlacodegen.utils import (
    get_column_names,
    get_constraint_sort_key,
    qualified_table_name,
)
from sqlalchemy import ForeignKeyConstraint
from sqlalchemy.sql.schema import Column, Table


def _is_directus_association_table(table: Table) -> bool:
    """
    Detect N:M Directus tables with the pattern:

    - Single-column primary key named 'id'
    - Exactly 2 ForeignKeyConstraints
    - All columns except 'id' participate in those FKs
    """
    # PK: single-column primary key named 'id'
    pk_cols = list(table.primary_key.columns)
    if len(pk_cols) != 1 or pk_cols[0].name != "id":
        return False

    # Exactly 2 ForeignKeyConstraints
    fk_constraints = [
        c for c in table.constraints
        if isinstance(c, ForeignKeyConstraint)
    ]
    if len(fk_constraints) != 2:
        return False

    fk_cols = {col for c in fk_constraints for col in c.columns}
    non_id_cols = {col for col in table.columns if col.name != "id"}

    return non_id_cols == fk_cols



class BafrapyDeclarativeGenerator(DeclarativeGenerator):
    """
    - If the column is a FK of a single column to <table>.id and its name matches
      the destination table (singularized if 'use_inflect') and does not end in '_id',
      rename the column attribute to '<name>_id'.
    - The relationship will automatically be named '<name>'.
    - Keep Directus simple N:M associations as is without creating Association Objects
    """

    def generate_models(self) -> list[Model]:
        models_by_table_name: dict[str, Model] = {}

        # Pick association tables from the metadata into their own set, don't process
        # them normally
        links: defaultdict[str, list[Model]] = defaultdict(lambda: [])

        for table in self.metadata.sorted_tables:
            qualified_name = qualified_table_name(table)

            # ---- Standard association + Directus N:M ----
            fk_constraints = sorted(
                table.foreign_key_constraints,
                key=get_constraint_sort_key,
            )

            # Original case: all columns have FK
            all_cols_are_fk = all(col.foreign_keys for col in table.columns)

            is_standard_association = (
                len(fk_constraints) == 2 and all_cols_are_fk
            )

            # New: Directus N:M table with PK 'id'
            is_directus_association = (
                len(fk_constraints) == 2 and _is_directus_association_table(table)
            )

            if is_standard_association or is_directus_association:
                model = models_by_table_name[qualified_name] = Model(table)
                tablename = fk_constraints[0].elements[0].column.table.name
                links[tablename].append(model)
                continue

            # ---- Same as DeclarativeGenerator ----

            # Only form model classes for tables that have a primary key and are not
            # association tables
            if not table.primary_key:
                models_by_table_name[qualified_name] = Model(table)
            else:
                model = ModelClass(table)
                models_by_table_name[qualified_name] = model

                # Fill in the columns
                for column in table.c:
                    column_attr = ColumnAttribute(model, column)
                    model.columns.append(column_attr)

        # Add relationships
        for model in models_by_table_name.values():
            if isinstance(model, ModelClass):
                self.generate_relationships(
                    model,
                    models_by_table_name,
                    links[model.table.name],
                )

        # Nest inherited classes in their superclasses to ensure proper ordering
        if "nojoined" not in self.options:
            for model in list(models_by_table_name.values()):
                if not isinstance(model, ModelClass):
                    continue

                pk_column_names = {col.name for col in model.table.primary_key.columns}
                for constraint in model.table.foreign_key_constraints:
                    if set(get_column_names(constraint)) == pk_column_names:
                        target = models_by_table_name[
                            qualified_table_name(constraint.elements[0].column.table)
                        ]
                        if isinstance(target, ModelClass):
                            model.parent_class = target
                            target.children.append(model)

        # Change base if we only have tables
        if not any(
            isinstance(model, ModelClass)
            for model in models_by_table_name.values()
        ):
            super().generate_base()

        # Collect the imports
        self.collect_imports(models_by_table_name.values())

        # Rename models and their attributes that conflict with imports or other
        # attributes
        global_names = {
            name for namespace in self.imports.values() for name in namespace
        }
        for model in models_by_table_name.values():
            self.generate_model_name(model, global_names)
            global_names.add(model.name)

        return list(models_by_table_name.values())


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

            if preferred == target_singular and not preferred.endswith("_id"):
                preferred = f"{preferred}_id"

        column_attr.name = self.find_free_name(preferred, global_names, local_names)


class BafrapySQLModelGenerator(SQLModelGenerator):
    """
    - If the column is a FK of a single column to <table>.id and its name matches
      the destination table (singularized if 'use_inflect') and does not end in '_id',
      rename the column attribute to '<name>_id'.
    - The relationship will automatically be named '<name>'.
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

            if preferred == target_singular and not preferred.endswith("_id"):
                preferred = f"{preferred}_id"

        column_attr.name = self.find_free_name(preferred, global_names, local_names)
