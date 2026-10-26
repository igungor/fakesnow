"""Parse Snowflake SQL with Snowflake-compatible syntax errors."""

from __future__ import annotations

from typing import ClassVar

import snowflake.connector.errors
import sqlglot
from sqlglot import Expr, exp
from sqlglot.dialects.snowflake import Snowflake
from sqlglot.errors import ParseError
from sqlglot.expressions.core import Expression
from sqlglot.parsers.snowflake import _show_parser
from sqlglot.tokenizer_core import TokenType
from sqlglot.trie import new_trie


class StageFiles(Expression):
    """The files of a stage that a statement acts on: a location, optionally with a PATTERN."""

    arg_types: ClassVar = {"this": True, "pattern": False}


class FakeSnowflake(Snowflake):
    """Snowflake's dialect with the statements sqlglot doesn't parse yet: CREATE, DESCRIBE, DROP and SHOW
    of pipes, SHOW GRANTS ON a table or view, and the stage and pattern of a REMOVE.

    A REMOVE stays a Command, so it generates as written, with its stage in the "remove_stage" arg and its
    pattern in "remove_pattern".
    """

    class Parser(Snowflake.Parser):
        SHOW_PARSERS: ClassVar = {
            **Snowflake.Parser.SHOW_PARSERS,
            "PIPES": _show_parser("PIPES"),
            "GRANTS": lambda self: self._parse_show_grants(),
        }
        SHOW_TRIE: ClassVar = new_trie(key.split(" ") for key in SHOW_PARSERS)
        EXPRESSION_PARSERS: ClassVar = {
            **Snowflake.Parser.EXPRESSION_PARSERS,
            StageFiles: lambda self: self._parse_stage_files(),
        }

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

        def _parse_show_grants(self) -> exp.Show | exp.Command:
            index = self._index
            if self._match(TokenType.ON) and self._match_texts(("TABLE", "VIEW")):
                kind = self._prev.text.upper()
                return self.expression(exp.Show(this="GRANTS", scope_kind=kind, scope=self._parse_table_parts()))

            # the other forms, eg: SHOW GRANTS TO ROLE, stay commands, from the SHOW they start with
            self._retreat(index)
            return self._parse_as_command(self._tokens[index - 2])

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

        def _parse_command(self) -> exp.Command:
            if self._prev.text.upper() not in ("REMOVE", "RM"):
                return super()._parse_command()

            command = self.expression(
                exp.Command(this=self._prev.text.upper(), expression=self._parse_string()),
                comments=self._prev_comments,
            )
            if not isinstance(rest := command.expression, exp.Literal):
                self.raise_error("Expected a stage")
            else:
                # the tokenizer keeps the rest of a REMOVE as a single string, so parse that string
                tokens = self.dialect.tokenize(rest.this)
                (files,) = self.dialect.parser(error_level=self.error_level).parse_into(StageFiles, tokens, rest.this)
                assert isinstance(files, StageFiles)
                command.set("remove_stage", files.this)
                command.set("remove_pattern", files.args.get("pattern"))
            return command

        def _parse_stage_files(self) -> StageFiles:
            if self._match(TokenType.STRING):
                # a quoted stage location, which may contain spaces
                location: exp.Expr = exp.var(self._prev.text)
            elif self._curr and self._curr.text.startswith("@"):
                location = self._parse_location_path()
            else:
                self.raise_error("Expected a stage")
                location = exp.var("")
            pattern = None
            if self._match_text_seq("PATTERN", "=") and not (pattern := self._parse_string()):
                self.raise_error("Expected string after PATTERN =")
            return self.expression(StageFiles(this=location, pattern=pattern))


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
