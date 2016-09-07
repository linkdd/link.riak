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
        When I connect to "kvstore+riak://localhost:8087/counters/testbucket?protocol=pbc"
        And I make sure it can store counters
        And I create a key "foo" with a counter starting at 5
        And I increment the counter "foo" by 6
        Then I have a counter "foo" starting at 11
        And I disconnect from the store
