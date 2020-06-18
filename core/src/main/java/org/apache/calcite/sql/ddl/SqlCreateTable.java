/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.sql.ddl;

import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;
import java.util.Objects;

/**
 * Parse tree for {@code CREATE TABLE} statement.
 */
public class SqlCreateTable extends SqlCreate {
  public final boolean temporary;
  public final boolean external;
  public final SqlIdentifier name;
  public final SqlNodeList columnList;
  public final SqlNode query;
  public final SqlNode comment;
  public final SqlNode rowFormat;
  public final SqlNode fileFormat;
  public final SqlNodeList partElementList;

  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("CREATE TABLE", SqlKind.CREATE_TABLE);

  protected SqlCreateTable(SqlParserPos pos, boolean replace, boolean ifNotExists,
      SqlIdentifier name, SqlNodeList columnList, SqlNode query) {
    super(OPERATOR, pos, replace, ifNotExists);
    this.temporary = false;
    this.external = false;
    this.name = Objects.requireNonNull(name);
    this.columnList = columnList; // may be null
    this.query = query; // for "CREATE TABLE ... AS query"; may be null
    this.comment = null;
    this.rowFormat = null;
    this.fileFormat = null;
    this.partElementList = null;
  }

  /** Creates a SqlCreateTable. */
  protected SqlCreateTable(SqlParserPos pos, boolean temporary, boolean external,
      boolean replace, boolean ifNotExists, SqlIdentifier name,
      SqlNodeList columnList, SqlNode query, SqlNode comment,
      SqlNode rowFormat, SqlNode fileFormat, SqlNodeList partElementList) {
    super(OPERATOR, pos, replace, ifNotExists);
    this.temporary = temporary;
    this.external = external;
    this.name = Objects.requireNonNull(name);
    this.columnList = columnList; // may be null
    this.query = query; // for "CREATE TABLE ... AS query"; may be null
    this.comment = comment;
    this.rowFormat = rowFormat;
    this.fileFormat = fileFormat;
    this.partElementList = partElementList;
  }

  public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(name, columnList, query, comment,
        rowFormat, fileFormat, partElementList);
  }

  public boolean isTemporary() {
    return this.temporary;
  }

  public boolean isExternal() {
    return this.external;
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("CREATE");
    if (temporary) {
      writer.keyword("TEMPORARY");
    }
    if (external) {
      writer.keyword("EXTERNAL");
    }
    writer.keyword("TABLE");
    if (ifNotExists) {
      writer.keyword("IF NOT EXISTS");
    }
    name.unparse(writer, leftPrec, rightPrec);
    if (columnList != null) {
      SqlWriter.Frame frame = writer.startList("(", ")");
      for (SqlNode c : columnList) {
        writer.sep(",");
        c.unparse(writer, 0, 0);
      }
      writer.endList(frame);
    }
    if (query != null) {
      writer.keyword("AS");
      writer.newlineAndIndent();
      query.unparse(writer, 0, 0);
    }

    if (comment != null) {
      writer.newlineAndIndent();
      writer.keyword("COMMENT");
      comment.unparse(writer, 0, 0);
    }

    if (rowFormat != null) {
      writer.newlineAndIndent();
      writer.keyword("ROW FORMAT");
      rowFormat.unparse(writer, 0, 0);
    }

    if (fileFormat != null) {
      writer.newlineAndIndent();
      writer.keyword("STORED AS");
      fileFormat.unparse(writer, 0, 0);
    }

    if (partElementList != null) {
      writer.newlineAndIndent();
      writer.keyword("PARTITIONED BY");
      SqlWriter.Frame frame = writer.startList("(", ")");
      for (SqlNode c : partElementList) {
        writer.sep(",");
        c.unparse(writer, 0, 0);
      }
      writer.endList(frame);
    }
  }
}
