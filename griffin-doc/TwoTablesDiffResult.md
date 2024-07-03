# Two tables diff result set
We want to unify result set for two table comparing, when two tables' schema are the same,
we can construct result set as below to let our users quickly find the difference between two tables.

| diff_type  | col1_src     | col1_target | col2_src  | col2_target | col3_src  | col3_target | col4_src   | col4_target |
|------------|--------------|-------------|-----------|-------------|-----------|-------------|------------|-------------|
| missing    | prefix1      | NULL        | sug_vote1 | NULL        | pv_total1 | NULL        | 2024-01-01 | NULL        |
| additional | NULL         | prefix1     | NULL      | sug_vote2   | NULL      | pv_total2   | NULL       | 2024-01-01  |
| missing    | prefix3      | NULL        | sug_vote3 | NULL        | pv_total3 | NULL        | 2024-01-03 | NULL        |
| additional | NULL         | prefix4     | NULL      | sug_vote4   | NULL      | pv_total3   | NULL       | 2024-01-03  |
| missing    | prefix5      | NULL        | sug_vote5 | NULL        | pv_total5 | NULL        | 2024-01-05 | NULL        |
| additional | NULL         | prefix5     | NULL      | sug_vote5   | NULL      | pv_total6   | NULL       | 2024-01-05  |
| missing    | prefix7      | NULL        | sug_vote7 | NULL        | pv_total7 | NULL        | 2024-01-07 | NULL        |
| additional | NULL         | prefix8     | NULL      | sug_vote8   | NULL      | pv_total8   | NULL       | 2024-01-07  |
| missing    | prefix9      | NULL        | sug_vote9 | NULL        | pv_total9 | NULL        | 2024-01-09 | NULL        |
| additional | NULL         | prefix10    | NULL      | sug_vote10  | NULL      | pv_total10  | NULL       | 2024-01-09  |
