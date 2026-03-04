# Data Index
A service to index transformed metadata for Collections, Objects, Agents, and Terms. It is intended to operate as part of a metadata transformation and indexing pipeline.

## Getting Started

With [git](https://git-scm.com/) installed, pull down the source code and move into the newly created directory:

```
git clone https://github.com/RockefellerArchiveCenter/data_index.git
cd data_index
```

## Service Flow

1. Recieves an SQS event message with a single object (record)
2. SQS triggers the Lambda
3. Iterates over each record in the event
4. Parses the JSON body and iterates over each object to read data
5. Groups objects by `object_type` and action (`add` or `delete`)
6. Executes bulk ElasticSearch indexing to index or delete
7. Publishes results messages to SNS

## Usage

This repository is intended to be deployed as a Lambda script in AWS infrastructure.

### Expected Message Format

The script is designed to consume batched messages as an event from an AWS Simple Queue Service (SQS) queue. Each SQS record contains a single object with the following structure:

### For index actions (`requested_action: "index"`):
- A JSON string message body containing an `objects` array (single element) with:
    - `es_id`: Elasticsearch document ID
    - `data`: Document body to be indexed, including:
        - `data.object_type`: Supported values: agent, collection, object, term
        - `data.uri`: unique object identifier
- `requested_action` message attribute: `"index"`

### For delete actions (`requested_action: "delete"`):
- A JSON string message body with:
    - `es_id`: Elasticsearch document ID to delete
    - `uri`: unique object identifier
- Message attributes:
    - `requested_action`: `"delete"`
    - `object_type`: Document type (agent, collection, object, or term)


## License

This code is released under the MIT License.

## Contributing

This is an open source project and we welcome contributions! If you want to fix a bug, or have an idea of how to enhance the application, the process looks like this:

1. File an issue in this repository. This will provide a location to discuss proposed implementations of fixes or enhancements, and can then be tied to a subsequent pull request.
2. If you have an idea of how to fix the bug (or make the improvements), fork the repository and work in your own branch. When you are done, push the branch back to this repository and set up a pull request. Automated unit tests are run on all pull requests. Any new code should have unit test coverage, documentation (if necessary), and should conform to the Python PEP8 style guidelines.
3. After some back and forth between you and core committers (or individuals who have privileges to commit to the base branch of this repository), your code will probably be merged, perhaps with some minor changes.

This repository contains a configuration file for git [pre-commit](https://pre-commit.com/) hooks which help ensure that code is linted before it is checked into version control. It is strongly recommended that you install these hooks locally by installing pre-commit and running `pre-commit install`.

## Tests

New code should have unit tests. Tests can be run using [tox](https://tox.readthedocs.io/).
