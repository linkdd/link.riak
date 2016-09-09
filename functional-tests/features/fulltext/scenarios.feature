Feature: FullText
    In order to query Riak with a fulltext search
    As developers
    We'll implement a fulltext feature

    Scenario: Search
        When I connect to "kvstore+riak://localhost:8087/fulltext/testbucket?protocol=pbc"
        And I can index riak with the schema "features/fulltext/schema.xml"
        And I have the following entries:
            | Key | Field1 | Field2  |
            | foo | woody  | buzz    |
            | bar | sarge  | etch    |
            | baz | lenny  | squeeze |
        Then I wait 5 seconds
        And I use the feature "fulltext"
        And I can search for "field1:(woody OR lenny)" and find the keys:
            | Key |
            | foo |
            | baz |
        And I disconnect from the store
