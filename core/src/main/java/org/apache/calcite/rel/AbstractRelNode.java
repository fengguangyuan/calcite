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
package org.apache.calcite.rel;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelDigest;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptQuery;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.metadata.Metadata;
import org.apache.calcite.rel.metadata.MetadataFactory;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.trace.CalciteTrace;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.apiguardian.api.API;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Base class for every relational expression ({@link RelNode}).
 */
public abstract class AbstractRelNode implements RelNode {
  //~ Static fields/initializers ---------------------------------------------

  /** Generator for {@link #id} values. */
  private static final AtomicInteger NEXT_ID = new AtomicInteger(0);

  private static final Logger LOGGER = CalciteTrace.getPlannerTracer();

  //~ Instance fields --------------------------------------------------------

  /**
   * Cached type of this relational expression.
   */
  protected RelDataType rowType;

  /**
   * The digest that uniquely identifies the node.
   */
  @API(since = "1.24", status = API.Status.INTERNAL)
  protected RelDigest digest;

  private final RelOptCluster cluster;

  /**
   * unique id of this object -- for debugging
   */
  protected final int id;

  /**
   * The RelTraitSet that describes the traits of this RelNode.
   */
  protected RelTraitSet traitSet;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates an <code>AbstractRelNode</code>.
   */
  public AbstractRelNode(RelOptCluster cluster, RelTraitSet traitSet) {
    super();
    assert cluster != null;
    this.cluster = cluster;
    this.traitSet = traitSet;
    this.id = NEXT_ID.getAndIncrement();
    this.digest = new RelDigest0();
  }

  //~ Methods ----------------------------------------------------------------

  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    // Note that empty set equals empty set, so relational expressions
    // with zero inputs do not generally need to implement their own copy
    // method.
    if (getInputs().equals(inputs)
        && traitSet == getTraitSet()) {
      return this;
    }
    throw new AssertionError("Relational expression should override copy. "
        + "Class=[" + getClass()
        + "]; traits=[" + getTraitSet()
        + "]; desired traits=[" + traitSet
        + "]");
  }

  protected static <T> T sole(List<T> collection) {
    assert collection.size() == 1;
    return collection.get(0);
  }

  @SuppressWarnings("deprecation")
  public List<RexNode> getChildExps() {
    return ImmutableList.of();
  }

  public final RelOptCluster getCluster() {
    return cluster;
  }

  public final Convention getConvention() {
    return traitSet.getTrait(ConventionTraitDef.INSTANCE);
  }

  public RelTraitSet getTraitSet() {
    return traitSet;
  }

  public String getCorrelVariable() {
    return null;
  }

  @Deprecated // to be removed before 1.25
  public boolean isDistinct() {
    final RelMetadataQuery mq = cluster.getMetadataQuery();
    return Boolean.TRUE.equals(mq.areRowsUnique(this));
  }

  @Deprecated // to be removed before 1.25
  public boolean isKey(ImmutableBitSet columns) {
    final RelMetadataQuery mq = cluster.getMetadataQuery();
    return Boolean.TRUE.equals(mq.areColumnsUnique(this, columns));
  }

  public int getId() {
    return id;
  }

  public RelNode getInput(int i) {
    List<RelNode> inputs = getInputs();
    return inputs.get(i);
  }

  @Deprecated // to be removed before 1.25
  public final RelOptQuery getQuery() {
    return getCluster().getQuery();
  }

  public void register(RelOptPlanner planner) {
    Util.discard(planner);
  }

  public final String getRelTypeName() {
    String cn = getClass().getName();
    int i = cn.length();
    while (--i >= 0) {
      if (cn.charAt(i) == '$' || cn.charAt(i) == '.') {
        return cn.substring(i + 1);
      }
    }
    return cn;
  }

  public boolean isValid(Litmus litmus, Context context) {
    return litmus.succeed();
  }

  @Deprecated // to be removed before 1.25
  public boolean isValid(boolean fail) {
    return isValid(Litmus.THROW, null);
  }

  /** @deprecated Use {@link RelMetadataQuery#collations(RelNode)} */
  @Deprecated // to be removed before 2.0
  public List<RelCollation> getCollationList() {
    return ImmutableList.of();
  }

  public final RelDataType getRowType() {
    if (rowType == null) {
      rowType = deriveRowType();
      assert rowType != null : this;
    }
    return rowType;
  }

  protected RelDataType deriveRowType() {
    // This method is only called if rowType is null, so you don't NEED to
    // implement it if rowType is always set.
    throw new UnsupportedOperationException();
  }

  public RelDataType getExpectedInputRowType(int ordinalInParent) {
    return getRowType();
  }

  public List<RelNode> getInputs() {
    return Collections.emptyList();
  }

  @Deprecated // to be removed before 1.25
  public final double getRows() {
    return estimateRowCount(cluster.getMetadataQuery());
  }

  public double estimateRowCount(RelMetadataQuery mq) {
    return 1.0;
  }

  @Deprecated // to be removed before 1.25
  public final Set<String> getVariablesStopped() {
    return CorrelationId.names(getVariablesSet());
  }

  public Set<CorrelationId> getVariablesSet() {
    return ImmutableSet.of();
  }

  public void collectVariablesUsed(Set<CorrelationId> variableSet) {
    // for default case, nothing to do
  }

  public boolean isEnforcer() {
    return false;
  }

  public void collectVariablesSet(Set<CorrelationId> variableSet) {
  }

  public void childrenAccept(RelVisitor visitor) {
    List<RelNode> inputs = getInputs();
    for (int i = 0; i < inputs.size(); i++) {
      visitor.visit(inputs.get(i), i, this);
    }
  }

  public RelNode accept(RelShuttle shuttle) {
    // Call fall-back method. Specific logical types (such as LogicalProject
    // and LogicalJoin) have their own RelShuttle.visit methods.
    return shuttle.visit(this);
  }

  public RelNode accept(RexShuttle shuttle) {
    return this;
  }

  @SuppressWarnings("deprecation")
  public final RelOptCost computeSelfCost(RelOptPlanner planner) {
    return computeSelfCost(planner, cluster.getMetadataQuery());
  }

  public RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    // by default, assume cost is proportional to number of rows
    double rowCount = mq.getRowCount(this);
    return planner.getCostFactory().makeCost(rowCount, rowCount, 0);
  }

  public final <M extends Metadata> M metadata(Class<M> metadataClass,
      RelMetadataQuery mq) {
    final MetadataFactory factory = cluster.getMetadataFactory();
    final M metadata = factory.query(this, mq, metadataClass);
    assert metadata != null
        : "no provider found (rel=" + this + ", m=" + metadataClass
        + "); a backstop provider is recommended";
    // Usually the metadata belongs to the rel that created it. RelSubset and
    // HepRelVertex are notable exceptions, so disable the assert. It's not
    // worth the performance hit to override this method for them.
    //   assert metadata.rel() == this : "someone else's metadata";
    return metadata;
  }

  public void explain(RelWriter pw) {
    explainTerms(pw).done(this);
  }

  /**
   * Describes the inputs and attributes of this relational expression.
   * Each node should call {@code super.explainTerms}, then call the
   * {@link org.apache.calcite.rel.externalize.RelWriterImpl#input(String, RelNode)}
   * and
   * {@link org.apache.calcite.rel.externalize.RelWriterImpl#item(String, Object)}
   * methods for each input and attribute.
   *
   * @param pw Plan writer
   * @return Plan writer for fluent-explain pattern
   */
  public RelWriter explainTerms(RelWriter pw) {
    return pw;
  }

  public RelNode onRegister(RelOptPlanner planner) {
    List<RelNode> oldInputs = getInputs();
    List<RelNode> inputs = new ArrayList<>(oldInputs.size());
    for (final RelNode input : oldInputs) {
      RelNode e = planner.ensureRegistered(input, null);
      assert e == input || RelOptUtil.equal("rowtype of rel before registration",
          input.getRowType(),
          "rowtype of rel after registration",
          e.getRowType(),
          Litmus.THROW);
      inputs.add(e);
    }
    RelNode r = this;
    if (!Util.equalShallow(oldInputs, inputs)) {
      r = copy(getTraitSet(), inputs);
    }
    r.recomputeDigest();
    assert r.isValid(Litmus.THROW, null);
    return r;
  }

  public RelDigest recomputeDigest() {
    digest.clear();
    return digest;
  }

  public void replaceInput(
      int ordinalInParent,
      RelNode p) {
    throw new UnsupportedOperationException("replaceInput called on " + this);
  }

  /** Description, consists of id plus digest */
  public String toString() {
    return "rel#" + id + ':' + getDigest();
  }

  /** Description, consists of id plus digest */
  @Deprecated // to be removed before 2.0
  public final String getDescription() {
    return this.toString();
  }

  public final String getDigest() {
    return digest.toString();
  }

  public final RelDigest getRelDigest() {
    return digest;
  }

  public RelOptTable getTable() {
    return null;
  }

  /**
   * {@inheritDoc}
   *
   * <p>This method (and {@link #hashCode} is intentionally final. We do not want
   * sub-classes of {@link RelNode} to redefine identity. Various algorithms
   * (e.g. visitors, planner) can define the identity as meets their needs.
   */
  @Override public final boolean equals(Object obj) {
    return super.equals(obj);
  }

  /**
   * {@inheritDoc}
   *
   * <p>This method (and {@link #equals} is intentionally final. We do not want
   * sub-classes of {@link RelNode} to redefine identity. Various algorithms
   * (e.g. visitors, planner) can define the identity as meets their needs.
   */
  @Override public final int hashCode() {
    return super.hashCode();
  }

  public boolean digestEquals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (this.getClass() != obj.getClass()) {
      return false;
    }
    AbstractRelNode that = (AbstractRelNode) obj;
    return this.getTraitSet() == that.getTraitSet()
        && this.getDigestItems().equals(that.getDigestItems())
        && Pair.right(getRowType().getFieldList()).equals(
        Pair.right(that.getRowType().getFieldList()))
        && (!(that instanceof Hintable)
            || ((Hintable) this).getHints().equals(
                ((Hintable) that).getHints()));
  }

  public int digestHash() {
    return Objects.hash(getTraitSet(), getDigestItems(),
        this instanceof Hintable ? ((Hintable) this).getHints() : null);
  }

  private List<Pair<String, Object>> getDigestItems() {
    RelDigestWriter rdw = new RelDigestWriter();
    explainTerms(rdw);
    return rdw.values;
  }

  private class RelDigest0 implements RelDigest {
    /**
     * Cache of hash code.
     */
    private int hash = 0;

    @Override public RelNode getRel() {
      return AbstractRelNode.this;
    }

    @Override public void clear() {
      hash = 0;
    }

    @Override public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final RelDigest0 relDigest = (RelDigest0) o;
      return digestEquals(relDigest.getRel());
    }

    @Override public int hashCode() {
      if (hash == 0) {
        hash = digestHash();
      }
      return hash;
    }

    @Override public String toString() {
      RelDigestWriter rdw = new RelDigestWriter();
      explain(rdw);
      return rdw.digest;
    }
  }

  /**
   * A writer object used exclusively for computing the digest of a RelNode.
   *
   * <p>The writer is meant to be used only for computing a single digest and then thrown away.
   * After calling {@link #done(RelNode)} the writer should be used only to obtain the computed
   * {@link #digest}. Any other action is prohibited.</p>
   *
   */
  private static final class RelDigestWriter implements RelWriter {

    private final List<Pair<String, Object>> values = new ArrayList<>();

    String digest = null;

    @Override public void explain(final RelNode rel, final List<Pair<String, Object>> valueList) {
      throw new IllegalStateException("Should not be called for computing digest");
    }

    @Override public SqlExplainLevel getDetailLevel() {
      return SqlExplainLevel.DIGEST_ATTRIBUTES;
    }

    @Override public RelWriter item(String term, Object value) {
      if (value != null && value.getClass().isArray()) {
        // We can't call hashCode and equals on Array, so
        // convert it to String to keep the same behaviour.
        value = "" + value;
      }
      values.add(Pair.of(term, value));
      return this;
    }

    @Override public RelWriter done(RelNode node) {
      StringBuilder sb = new StringBuilder();
      sb.append(node.getRelTypeName());
      sb.append('.');
      sb.append(node.getTraitSet());
      sb.append('(');
      int j = 0;
      for (Pair<String, Object> value : values) {
        if (j++ > 0) {
          sb.append(',');
        }
        sb.append(value.left);
        sb.append('=');
        if (value.right instanceof RelNode) {
          RelNode input = (RelNode) value.right;
          sb.append(input.getRelTypeName());
          sb.append('#');
          sb.append(input.getId());
        } else {
          sb.append(value.right);
        }
      }
      sb.append(')');
      digest = sb.toString();
      return this;
    }
  }
}
