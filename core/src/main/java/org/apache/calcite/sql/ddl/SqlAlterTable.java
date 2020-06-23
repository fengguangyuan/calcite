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

import org.apache.calcite.sql.SqlAlter;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.google.common.collect.ImmutableList;

import java.util.List;
import javax.annotation.Nonnull;

/**
 * Parse tree for {@code ALTER TABLE} statement.
 */
public class SqlAlterTable extends SqlAlter {
  public final SqlIdentifier name;
  public final SqlNodeList columnList;
  public final boolean cascade;

  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("ALTER TABLE", SqlKind.ALTER_TABLE);

  public SqlAlterTable(SqlParserPos pos, String scope, SqlIdentifier name,
      SqlNodeList columnList, boolean cascade) {
    super(pos, scope);
    this.name = name;
    this.columnList = columnList;
    this.cascade = cascade;
  }

  @Override protected void unparseAlterOperation(SqlWriter writer, int leftPrec, int rightPrec) {
    name.unparse(writer, leftPrec, rightPrec);
    writer.newlineAndIndent();
    writer.keyword("ADD");
    writer.keyword("COLUMNS");
    if (columnList != null) {
      SqlWriter.Frame frame = writer.startList("(", ")");
      for (SqlNode c : columnList) {
        writer.sep(",");
        c.unparse(writer, 0, 0);
      }
      writer.endList(frame);
    }
    if (cascade) {
      writer.newlineAndIndent();
      writer.keyword("CASCADE");
    }
  }

  public boolean isCascade() {
    return cascade;
  }

  @Nonnull @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Nonnull @Override public List<SqlNode> getOperandList() {
    return ImmutableList.of(name, columnList);
  }

}
