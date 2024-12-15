import sqlalchemy


class aggregate_function_call(sqlalchemy.sql.expression.ColumnElement):
    """
    Compiler construct for BigQuery "aggregate function calls". Derived from the implementation
    of `aggregate_order_by` in the Postgres dialect.

    Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate-function-calls
    Ref: https://docs.sqlalchemy.org/en/20/dialects/postgresql.html#sqlalchemy.dialects.postgresql.aggregate_order_by
    """

    __visit_name__ = "aggregate_function_call"
    inherit_cache = False

    _traverse_internals = [
        ("target", sqlalchemy.sql.visitors.InternalTraversal.dp_clauseelement),
        ("type", sqlalchemy.sql.visitors.InternalTraversal.dp_type),
        ("order_by", sqlalchemy.sql.visitors.InternalTraversal.dp_clauseelement),
    ]

    def __init__(self, target, *, ignore_nulls=None, order_by=None, limit=None):
        self.target = sqlalchemy.sql.coercions.expect(
            sqlalchemy.sql.roles.ExpressionElementRole, target
        )
        self.type = self.target.type

        self.ignore_nulls = ignore_nulls
        self.order_by = None
        self.limit = None

        if order_by is not None and len(order_by) > 0:
            self.order_by = (
                sqlalchemy.sql.coercions.expect(
                    sqlalchemy.sql.roles.ExpressionElementRole, order_by[0]
                )
                if len(order_by) == 1
                else sqlalchemy.sql.elements.ClauseList(
                    *order_by,
                    _literal_as_text_role=sqlalchemy.sql.roles.ExpressionElementRole
                )
            )
        if limit is not None:
            self.limit = sqlalchemy.sql.coercions.expect(
                sqlalchemy.sql.roles.LimitOffsetRole,
                limit,
                name=None,
                type_=sqlalchemy.sql.sqltypes.Integer,
            )

    def self_group(self, against=None):
        return self

    def get_children(self, **kw):
        children = [self.target]
        if self.order_by is not None:
            children.append(self.order_by)
        return children

    def _copy_internals(self, clone=sqlalchemy.sql.elements._clone, **kw):
        self.target = clone(self.target, **kw)
        self.type = self.target.type
        if self.order_by is not None:
            self.order_by = clone(self.order_by, **kw)

    @property
    def _from_objects(self):
        from_objects = self.target._from_objects
        if self.order_by is not None:
            from_objects += self.order_by._from_objects
        return from_objects
