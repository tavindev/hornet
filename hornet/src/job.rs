#[derive(Debug)]
pub struct Job<Data> {
    pub id: String,
    pub name: String,
    pub data: Data,
    pub opts: String,
    pub timestamp: u128,
    pub delay: u128,
    pub priority: u32,
    pub processed_on: u128,
    pub ats: u32,
}
