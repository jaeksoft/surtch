#[cfg(test)]
mod tests {
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
        document2.field("id").term("id2", 1);
        document2.field("title").term("my", 0).term("second", 1).term("title", 2).term("titles", 2);
        documents.push(document2);

        let mut document3 = Document::new();
        document3.field("id").term("id3", 1);
        document3.field("title").term("my", 0).term("third", 1).term("title", 2).term("titles", 2);
        document3.field("content").term("the", 0).term("content", 1).term("of", 2).term("the", 3).term("document", 4).term("of", 5);
        documents.push(document3);

        let mut document4 = Document::new();
        document4.field("id").term("id4", 1);
        document4.field("title").term("my", 0).term("four", 1).term("title", 2).term("titles", 2);
        document4.field("content").term("the", 0).term("content", 1).term("of", 2).term("the", 3).term("document", 4).term("of", 5);
        documents.push(document4);

        assert!(index.create_segment(&documents).is_ok());
    }

    #[test]
    fn fail_on_create_index_sub_directory() {
        let result = Index::new("target/test/test/test");
        assert!(result.is_err());
    }
}
