/*
 * Copyright (c) 2022 nosqlbench
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

package io.nosqlbench.cqlgen.exporter.transformers;

import io.nosqlbench.cqlgen.model.CqlColumnDef;
import io.nosqlbench.cqlgen.model.CqlModel;
import io.nosqlbench.cqlgen.model.CqlTable;

import java.util.List;

public class CGUdtReplacer implements CGModelTransformer {

    @Override
    public CqlModel apply(CqlModel model) {
        List<String> toReplace = model.getTypeDefs().stream().map(t -> t.getKeyspace() + "." + t.getName()).toList();
        for (CqlTable table : model.getTableDefs()) {
            for (CqlColumnDef coldef : table.getColumnDefinitions()) {
                String typedef = coldef.getTrimmedTypedef();
                for (String searchFor : toReplace) {
                    if (typedef.contains(searchFor)) {
                        coldef.setTypeDef("blob");
                    }
                }
            }
        }

        return model;
    }


}