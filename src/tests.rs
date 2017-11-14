#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use index::index::Index;
    use document::document::Document;

    #[test]
    fn create_index() {
        let index = Index::new("target/test").unwrap();
        assert_eq!(index.path, "target/test");

        let mut documents = Vec::new();

        let mut document1 = Document::new();
        document1.field("id").term("id1", 0);
        document1.field("title").term("my", 0).term("title", 1);
        documents.push(document1);

        let mut document2 = Document::new();
        document2.field("id").term("id1", 1);
        document2.field("title").term("my", 0).term("second", 1).term("title", 2).term("titles", 2);
        documents.push(document2);

        assert!(index.insert(documents).is_ok());
    }

    #[test]
    fn fail_on_create_index_sub_directory() {
        let result = Index::new("target/test/test/test");
        assert!(result.is_err());
    }
}
