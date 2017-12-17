# surtch

![Travis](https://travis-ci.org/jaeksoft/surtch.svg?branch=master)

![Codecov](https://codecov.io/gh/jaeksoft/surtch/branch/master/graph/badge.svg)

### File structure:

- {snapshot_uuid}.cat: Catalog of field/segment
- {field_name}/{segment_uuid}/fst : Map of Term/Idx
- {field_name}/{segment_uuid}.dox : Array of DocOffset (Fixed size)
- {field_name)/{segment_uuid}.doc: : Array of DocIds (dynamic bitset)
- {field_name)/{segment_uuid}.pox: : Array of PositionsOffsets (Fixed size)
- {field_name)/{segment_uuid}.pos: : Array of Positions (dynamic arrays)
