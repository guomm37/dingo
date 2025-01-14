/*
 * Copyright 2021 DataCanvas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.dingodb.calcite.rule;

import io.dingodb.calcite.DingoTable;
import io.dingodb.calcite.grammar.SqlUserDefinedOperators;
import io.dingodb.calcite.rel.LogicalDingoDocument;
import io.dingodb.common.type.TupleMapping;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

@Value.Enclosing
public class DingoDocumentProjectRule extends RelRule<DingoDocumentProjectRule.Config> implements SubstitutionRule {

    public DingoDocumentProjectRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalProject project = call.rel(0);
        final LogicalDingoDocument document = call.rel(1);
        final List<Integer> selectedColumns = new ArrayList<>();
        final List<RexCall> documentSelected = new ArrayList<>();
        final RexVisitorImpl<Void> visitor = new RexVisitorImpl<Void>(true) {
            @Override
            public @Nullable Void visitInputRef(@NonNull RexInputRef inputRef) {
                if (!selectedColumns.contains(inputRef.getIndex())) {
                    selectedColumns.add(inputRef.getIndex());
                }
                return null;
            }

            @Override
            public Void visitCall(RexCall call) {
                return super.visitCall(call);
            }
        };
        List<RexNode> projects = project.getProjects();
        RexNode filter = document.getFilter();
        visitor.visitEach(projects);
        if (filter != null) {
            filter.accept(visitor);
        }

        /*
          select id, distance from xx where not in (sub item size > 20)  -> generate same node to infinite loop
          join
          left: document scan -> logicalProject -> filter (cause infinite loop)
          right: agg values
          so break
         */
        if (projects.size() > selectedColumns.size()) {
            return;
        }
        // Order naturally to help decoding in push down.
        selectedColumns.sort(Comparator.naturalOrder());
        Mapping mapping = Mappings.target(selectedColumns, document.getRowType().getFieldCount());

        LogicalDingoDocument newDocument = new LogicalDingoDocument(
            document.getCluster(),
            document.getTraitSet(),
            document.getCall(),
            document.getTable(),
            document.getOperands(),
            document.getIndexTableId(),
            document.getIndexTable(),
            TupleMapping.of(selectedColumns),
            filter,
            document.hints,
            document.isDocumentScanFilter()
        );
        final List<RexNode> newProjectRexNodes = RexUtil.apply(mapping, project.getProjects());
        if (RexUtil.isIdentity(newProjectRexNodes, newDocument.getSelectedType())) {
            call.transformTo(newDocument);
        } else {
            if (!documentSelected.isEmpty()) {
                return;
            }
            call.transformTo(
                new LogicalProject(
                    project.getCluster(),
                    project.getTraitSet(),
                    project.getHints(),
                    newDocument,
                    newProjectRexNodes,
                    project.getRowType(),
                    project.getVariablesSet()
                )
            );
        }
    }

    @Override
    public boolean autoPruneOld() {
        return false;
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        DingoDocumentProjectRule.Config DEFAULT = ImmutableDingoDocumentProjectRule.Config.builder()
            .operandSupplier(b0 ->
                b0.operand(LogicalProject.class).oneInput(b1 ->
                    b1.operand(LogicalDingoDocument.class)
                        .predicate(rel -> {
                            if (rel.getRealSelection() == null) {
                                return true;
                            }
                            DingoTable dingoTable = rel.getTable().unwrap(DingoTable.class);
                            assert dingoTable != null;
                            return rel.getRealSelection().size() == dingoTable.getTable().getColumns().size() + 1;
                        } )
                        .noInputs()
                )
            )
            .description("DingoDocumentProjectRule")
            .build();

        @Override
        default DingoDocumentProjectRule toRule() {
            return new DingoDocumentProjectRule(this);
        }
    }
}
