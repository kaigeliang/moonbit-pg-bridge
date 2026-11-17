// Learn more about moon.mod configuration:
// https://docs.moonbitlang.com/en/latest/toolchain/moon/module.html
//
// To add a dependency, run this command in your terminal:
//   moon add moonbitlang/x
//
// Or manually declare it in `import`, for example:
// import {
//   "moonbitlang/x@0.4.6",
// }

name = "kaigeliang/moonbit-pg-bridge"

version = "0.1.0"

readme = "README.mbt.md"

repository = "https://github.com/kaigeliang/moonbit-pg-bridge"

license = "Apache-2.0"

keywords = [ ]

preferred_target = "native"

description = "Experimental community PostgreSQL adapter for moondb"

import {
  "moonbitstack/moondb@0.3.0",
  "moonbit-community/postgres@0.1.3",
  "moonbitlang/async@0.22.5",
}
