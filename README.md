# eloquent-clojure-logger

An alternative Fluentd logger. Just a side-project, so don't
expect it to be full-featured. Some planned features are

* Buffering
* ACK support
* Nanosecond time (wish)
* SSL support

## Usage

    user=> (require '[eloquent-clojure-logger.core :refer :all])
    nil
    user=> (def c (eloquent-client :tag "my.tag"))
    #'user/c
    user=> (eloquent-log c {"message" "logging..."})
    true
    user=> 

## License

Copyright © 2017 Nathaniel C. Domingo

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
