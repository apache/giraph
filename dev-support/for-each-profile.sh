#!/bin/bash
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.


if [[ -z `which xpath 2> /dev/null` ]]; then
  echo "ERROR: xpath not found in path, install xpath first"
  exit
fi

DIR=$(cd "$(dirname "$0")" && pwd)
TOP_DIR=$DIR/..

function echo_separator {
  echo "======================"
}

function parse_profiles {
  all_profiles=""
  poms=$(find $TOP_DIR -name pom.xml)
  for pom in $poms; do
    # Returns something like:
    #   <id>hadoop_0.20.203</id><id>hadoop_1.0</id><id>hadoop_non_secure</id> ...
    profiles=$(xpath $pom '//project/profiles/profile/id' 2> /dev/null)

    # Transform into:
    #   hadoop_0.20.203 hadoop_1.0 ...
    profiles=${profiles//<\/id>/}
    profiles=${profiles//<id>/ }

    # Append to accumulating list
    all_profiles="$all_profiles $profiles"
  done

  # Sort and uniq the whole list
  all_profiles=$(echo "$all_profiles" | tr -s ' ' "\n" | sort | uniq)
}

parse_profiles

echo "Running on profiles: $all_profiles"

results_file=$TOP_DIR/for-each-profile-results.txt

echo "mvn -P<profile> $@" > $results_file
echo "======================================================" >> $results_file

for profile in $all_profiles; do
  echo_separator
  echo "=== $profile ==="
  echo_separator

  args="-P$profile $@"

  mvn $args
  result=$?

  if [[ $result -ne 0 ]]; then
    echo_separator
    echo "=== Failed on profile: $profile"
    echo "=== Failed command: mvn $args"
    echo "$profile: FAILED" >> $results_file
    echo_separator
  else
    echo "$profile: SUCCESS" >> $results_file
  fi
done
