Feature: KeyValueStore
    In order to interact with Riak with a dict-like API
    As developers
    We'll implement a KeyValueStore Driver

    Scenario: CRUD
        When I connect to "kvstore+riak://localhost:8087/testbucket?protocol=pbc"
        And I create a key "foo" with the JSON document "features/driver/crud1.json"
        Then I can find the key "foo"
        And I can get the key "foo" which match the value in "features/driver/crud1.json"
        And I can update the key "foo" with the JSON document "features/driver/crud2.json"
        Then I can get the key "foo" which match the value in "features/driver/crud2.json"
        And I can delete the key "foo"
        Then I cannot find the key "foo"
        And I disconnect from the store

    Scenario: CRDT Counter
        When I make sure riak can store counters
        And I connect to "kvstore+riak://localhost:8087/counters/testbucket?protocol=pbc"
        And I create a counter "foo" starting at 5
        And I increment the counter "foo" by 6
        Then I have a counter "foo" starting at 11
        And I disconnect from the store

    Scenario: CRDT Set
        When I make sure riak can store sets
        And I connect to "kvstore+riak://localhost:8087/sets/testbucket?protocol=pbc"
        And I create a set "foo" containing "bar" and "baz"
        And I add "biz" to the set "foo"
        Then I have a set "foo" containing "bar" and "baz" and "biz"
        When I discard "bar" from the set "foo"
        Then I have a set "foo" containing "baz" and "biz"
        And I disconnect from the store

    Scenario: CRDT Map
        When I make sure riak can store maps
        And I connect to "kvstore+riak://localhost:8087/maps/testbucket?protocol=pbc"
        And I create a map "foo" containing a flag "f" enabled by default
        And I increment a counter "c" by 5 in the map "foo"
        And I set a register "r" to "hello world" in the map "foo"
        And I add to a set "s" in the map "foo":
            | Value |
            | foo   |
            | bar   |
            | baz   |
        And I add to a map "m" a counter "c" incremented by 6 in the map "foo"
        Then I have a map "foo" which match the value in "features/driver/crdt.map.json"
        And I disconnect from the store
