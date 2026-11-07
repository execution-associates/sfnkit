"""Wire format between state machines and step runtimes. Both sides must agree on every key here.

ENVELOPE (the Task payload a step receives)::

    {
      "sw": 1,
      "step": "myapp.flows.fetch",        # Step.key (module + qualname); the runtime imports the module
      "execution": "<execution name>",    # $states.context.Execution.Name
      "node": "n12",                      # IR node id, used in S3 keys
      "args": {"url": <value>, ...},      # values are plain JSON or any wrapper below (resolved first)
      "facts": {"k_1a2b": ["score"]},     # optional: fact key -> path into the return value
      "items": {                          # optional: list facets for Map states
        "k_3c4d": {"path": ["urls"], "mode": "inline" | "s3", "facts": {"k_5e6f": ["id"]}}
      },
      "index": 3                          # optional: Map item index
    }

RESULT WRAPPER (what a step returns to Step Functions)::

    {"$sw": 1, "v": <value>}             # value inline, or
    {"$sw": 1, "s3": "s3://b/k"}         # value offloaded (JSON object at that key)
      + "f":  {"k_1a2b": <value at path or null>}            # when "facts" was requested
      + "it": {"k_3c4d": [<element wrapper>, ...]}           # facet mode "inline", or
              {"k_3c4d": {"bucket": "b", "key": "k"}}        # facet mode "s3": a JSON array file of
                                                             #   element wrappers
    element wrapper = {"$sw": 1, "v"|"s3": ..., "f": {...}?}

Offloading: a value whose JSON is over the threshold goes to
``s3://<SFNKIT_BUCKET>/runs/<execution>/<node>/<uuid>.json``. Elements in an "inline" facet are
offloaded one by one so the list itself stays small.

OTHER WRAPPERS the state machine builds (the runtime and client must resolve all of them, recursively):

    {"$sw": 1, "x": <value>, "f": {...}, "i": 3}   # carrier: resolve(x); "f"/"i" are for the ASL only
    {"$sw": 1, "pj": <value>, "p": ["a", 0]}       # projection: resolve(pj)[a][0]
    {"$sw": 1, "c": "<Cause string>"}              # error cause: JSON-decoded if it parses, else string
    {"$sw": 1, "dmap": {"Bucket": b, "Key": k}}    # Distributed Map ResultWriter manifest: read every
                                                   #   SUCCEEDED/FAILED result file, take each child's
                                                   #   Output (a JSON string of a carrier with "i"),
                                                   #   order by "i", None for failed items (index taken
                                                   #   from the child Input's "index")
    {"$sw": 1, "v": [<wrapper>, ...]}              # inline Map output: resolve each element

Resolution of "v" recurses into the value, so wrappers nested in lists/dicts resolve too. The key
"$sw" is reserved: user data must not use it.
"""

VERSION = 1
MARK = "$sw"

# result wrapper keys
VALUE = "v"
S3 = "s3"
FACTS = "f"
ITEMS = "it"
# ASL-built wrapper keys
CARRIED = "x"
INDEX = "i"
PROJ = "pj"
PROJ_PATH = "p"
CAUSE = "c"
DMAP = "dmap"

RUNS_PREFIX = "runs/"

ENV_MODULE = "SFNKIT_MODULE"
ENV_BUCKET = "SFNKIT_BUCKET"
ENV_THRESHOLD = "SFNKIT_OFFLOAD_THRESHOLD"
