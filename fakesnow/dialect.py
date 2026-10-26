"""Parse Snowflake SQL with Snowflake-compatible syntax errors."""

from __future__ import annotations

from typing import ClassVar

import snowflake.connector.errors
import sqlglot
from sqlglot import Expr, exp
from sqlglot.dialects.snowflake import Snowflake
from sqlglot.errors import ParseError
from sqlglot.parsers.snowflake import _show_parser
from sqlglot.tokenizer_core import TokenType
from sqlglot.trie import new_trie


class FakeSnowflake(Snowflake):
    """Snowflake's dialect with the statements sqlglot doesn't parse yet: CREATE, DESCRIBE, DROP and SHOW
    of pipes."""

    class Parser(Snowflake.Parser):
        SHOW_PARSERS: ClassVar = {**Snowflake.Parser.SHOW_PARSERS, "PIPES": _show_parser("PIPES")}
        SHOW_TRIE: ClassVar = new_trie(key.split(" ") for key in SHOW_PARSERS)

        def _parse_create(self) -> exp.Create | exp.Command:
            index = self._index
            replace = self._prev.token_type == TokenType.REPLACE or self._match_pair(TokenType.OR, TokenType.REPLACE)

            if not self._match_text_seq("PIPE"):
                self._retreat(index)
                return super()._parse_create()

            exists = self._parse_exists(not_=True)
            this = self._parse_table_parts(schema=True)
            properties = self._parse_properties()
            if not self._match(TokenType.ALIAS):
                self.raise_error("Expected AS")

            # snowflake keeps the definition as written, and clients compare it to what they created
            start = self._curr
            copy = self._parse_statement()
            if not isinstance(copy, exp.Copy):
                self.raise_error("Expected COPY INTO")

            create = self.expression(
                exp.Create(
                    this=this, kind="PIPE", replace=replace, exists=exists, properties=properties, expression=copy
                )
            )
            create.set("pipe_definition", self._find_sql(start, self._prev))
            return create

        def _parse_describe(self) -> exp.Describe:
            if self._match_text_seq("PIPE"):
                return self.expression(exp.Describe(this=self._parse_table_parts(schema=True), kind="PIPE"))
            return super()._parse_describe()

        def _parse_drop(self, exists: bool = False, kind: str | None = None) -> exp.Drop | exp.Command:
            if not kind and self._match_text_seq("PIPE"):
                if_exists = exists or self._parse_exists()
                return self.expression(
                    exp.Drop(tables=[self._parse_table_parts(schema=True)], kind="PIPE", exists=if_exists)
                )
            return super()._parse_drop(exists=exists, kind=kind)


def parse(command: str) -> list[Expr | None]:
    try:
        return sqlglot.parse(command, read=FakeSnowflake)
    except ParseError as error:
        raise syntax_error(error) from None


def parse_one(command: str) -> Expr:
    try:
        return sqlglot.parse_one(command, read=FakeSnowflake)
    except ParseError as error:
        raise syntax_error(error) from None


def syntax_error(error: ParseError) -> snowflake.connector.errors.ProgrammingError:
    # Strip terminal highlighting from SQLGlot's error message.
    message = str(error).replace("\x1b[4m", "").replace("\x1b[0m", "")
    return snowflake.connector.errors.ProgrammingError(msg=message, errno=1003, sqlstate="42000")
