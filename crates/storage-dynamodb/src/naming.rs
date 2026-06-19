//! Maps ExtendDB's (account_id, logical table name) to a flat physical DynamoDB
//! table name. ExtendDB is multi-tenant; DynamoDB tables are flat per AWS
//! account, so we namespace: `<prefix><account_id>_<table>`. Default prefix is
//! `athome_`, because of course it is.

#[derive(Debug, Clone)]
pub struct Namer {
    prefix: String,
}

impl Namer {
    pub fn new(prefix: &str) -> Self {
        Self { prefix: prefix.to_owned() }
    }

    /// `<prefix><account_id>_<table>`
    pub fn physical(&self, account_id: &str, table: &str) -> String {
        format!("{}{}_{}", self.prefix, account_id, table)
    }

    /// Inverse of `physical`, scoped to one account. Errors if `physical` does
    /// not belong to `account_id`.
    pub fn logical(&self, account_id: &str, physical: &str) -> Result<String, String> {
        let want = format!("{}{}_", self.prefix, account_id);
        physical
            .strip_prefix(&want)
            .map(|s| s.to_owned())
            .ok_or_else(|| format!("physical table '{physical}' not in account '{account_id}'"))
    }

    /// The account-scoped prefix used to filter ListTables results.
    pub fn account_prefix(&self, account_id: &str) -> String {
        format!("{}{}_", self.prefix, account_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn physical_name_combines_prefix_account_table() {
        let n = Namer::new("athome_");
        assert_eq!(n.physical("123456789012", "Orders"), "athome_123456789012_Orders");
    }

    #[test]
    fn logical_name_round_trips() {
        let n = Namer::new("athome_");
        let phys = n.physical("123456789012", "Orders");
        assert_eq!(n.logical("123456789012", &phys).unwrap(), "Orders");
    }

    #[test]
    fn logical_name_rejects_foreign_account() {
        let n = Namer::new("athome_");
        let phys = n.physical("111111111111", "Orders");
        assert!(n.logical("222222222222", &phys).is_err());
    }

    #[test]
    fn logical_name_preserves_underscores_in_table() {
        let n = Namer::new("athome_");
        let phys = n.physical("123456789012", "my_orders_v2");
        assert_eq!(n.logical("123456789012", &phys).unwrap(), "my_orders_v2");
    }
}
