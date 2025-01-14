# Issue Report

## 1.Submission Guidelines
```shell
[type][module]<description>
```
## 2.Guidelines Description
### 2.1 Description of Guidelines
 * type:Operation type, written in all lowercase
 * module:Modified module, written in all lowercase
 * description:Problem description, written with the first letter capitalized
 * If a single commit involves multiple modules, use the module with the most modifications as the final module
 * For commits related to proto interfaces, be sure to submit them separately
 * In principle, one commit should solve one problem; each submission should involve no more than 100 files

 ### 2.2 Parameter Description

 <table>
  <tr>
    <th>Parameter Type</th>
    <th>Parameter Range</th>
    <th>Parameter Description</th>
  </tr>
  <tr>
    <td rowspan="7">type</td>
    <td>feat</td>
    <td>New feature, meaning the implementation of new requirements.</td>
  </tr>
  <tr>
    <td>fix</td>
    <td>Fix, addressing a bug.</td>
  </tr>
  <tr>
    <td>doc</td>
    <td>Documentation, adding or updating documentation.</td>
  </tr>
  <tr>
    <td>style</td>
    <td>Style adjustment, primarily for code style-related submissions, such as formatting.</td>
  </tr>
  <tr>
    <td>refactor</td>
    <td>Refactoring code, restructuring existing functionality.</td>
  </tr>
  <tr>
    <td>test</td>
    <td>Unit test, adding code related to unit tests.</td>
  </tr>
  <tr>
    <td>chore</td>
    <td>Integration and deployment related tasks.</td>
  </tr>
  <tr>
    <td rowspan="9">module</td>
    <td>coordinator</td>
    <td>Metadata management related.</td>
  </tr>
  <tr>
    <td>store</td>
    <td>Distributed storage-related, such as RocksDB, Raft-KV-Engine.</td>
  </tr>
  <tr>
    <td>sdk</td>
    <td>Client interfaces.</td>
  </tr>
  <tr>
    <td>deploy</td>
    <td>Deployment script related.</td>
  </tr>
  <tr>
    <td>common</td>
    <td>Basic modules, such as CMake restructuring.</td>
  </tr>
  <tr>
    <td>proto</td>
    <td>pProto-related Protobuf modifications.</td>
  </tr>
  <tr>
    <td>index</td>
    <td>Index-related, involving scalar and vector indexing.</td>
  </tr>
  <tr>
    <td>executor</td>
    <td>Executor layer-related.</td>
  </tr>
    <tr>
    <td>rules</td>
    <td>Product specifications related.</td>
  </tr>
  <tr>
    <td>description</td>
    <td colspan="2">Description of the modifications in this commit, requirements: 1) All in English; 2) First letter capitalized</td>
  </tr>
</table>

*Example：*
```shell
[feat][coordinator] Add interface to metadata.
```
## 3. Submission Conditions
 * Ensure that all integration tests pass in advance.
 
