#!/bin/bash

if [[ -z `which xpath 2> /dev/null` ]]; then
  echo "ERROR: xpath not found in path"
  exit
fi

function parse_profiles {
  all_profiles=""
  poms=$(find . -name pom.xml)
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

for profile in $all_profiles; do
  echo "======================"
  echo "=== $profile ==="
  echo "======================"

  mvn -P$profile $@
  result=$?

  if [[ $result -ne 0 ]]; then
    echo "======================"
    echo "=== Failed on profile: $profile"
    echo "=== Failed command: mvn -P$profile $@"
    echo "======================"
    exit $result
  fi
done
