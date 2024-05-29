use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "grimoire.pest"]
pub struct GrimoireParser {}
