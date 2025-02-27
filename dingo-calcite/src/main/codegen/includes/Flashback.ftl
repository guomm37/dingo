<#--
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
-->


SqlFlashBack SqlFlashBack(): {
  final Span s;
  final SqlFlashBack sqlFlashback;
  final SqlIdentifier tableId;
  SqlIdentifier newTableId = null;
  SqlIdentifier schemaId = null;
} {
  <FLASHBACK> { s = span(); }
  (
   <TABLE> tableId = CompoundIdentifier()
    [ <TO>  newTableId = CompoundIdentifier() ]
    {
      return new SqlFlashBackTable(s.end(this), tableId, newTableId);
    }
  |
   (<SCHEMA>|<DATABASE>) schemaId = CompoundIdentifier()
    {
      return new SqlFlashBackSchema(s.end(this), schemaId);
    }
  )

}

SqlRecoverTable SqlRecoverTable(): {
  final Span s;
  final SqlRecoverTable sqlRecoverTable;
  final SqlIdentifier tableId;
  SqlIdentifier newTableId = null;
} {
  <RECOVER> { s = span(); }
   <TABLE> tableId = CompoundIdentifier()
    [ <TO>  newTableId = CompoundIdentifier() ]
    {
      return new SqlRecoverTable(s.end(this), tableId, newTableId);
    }

}
