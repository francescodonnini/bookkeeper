#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." &> /dev/null && pwd)"

echo "SCRIPT_DIR=  $SCRIPT_DIR"
echo "PROJECT_ROOT=$PROJECT_ROOT"
echo "CWD=         $PROJECT_ROOT"

cd "$PROJECT_ROOT"

EVOSUITE_JAR="src/test/resources/evosuite-1.2.0.jar"
EVOSUITE_URL="https://github.com/EvoSuite/evosuite/releases/download/v1.2.0/evosuite-1.2.0.jar"
CLEAN_CP="target/clean-cp"
TARGET_CLASSES="target/classes"
TEST_DIR="src/test/java"
REPORT_DIR="src/test/resources/evosuite-report"

if [ ! -f "$EVOSUITE_JAR" ]; then
    echo "[] EvoSuite JAR not found. Downloading v1.2.0..."
    wget -O "$EVOSUITE_JAR" "$EVOSUITE_URL"
else
    echo "[] EvoSuite JAR found."
fi

mvn clean compile test-compile
rm -rf "$CLEAN_CP"
mkdir -p "$CLEAN_CP"
mvn dependency:unpack-dependencies -DoutputDirectory="$CLEAN_CP"
rm -rf "$CLEAN_CP/META-INF/versions"

CLASSES=(
  "org.apache.bookkeeper.bookie.BufferedChannel"
  "org.apache.bookkeeper.bookie.storage.ldb.WriteCache"
)

for CLASS in "${CLASSES[@]}"; do
  java -jar "$EVOSUITE_JAR" \
    -class "$CLASS" \
    -projectCP "$TARGET_CLASSES:$CLEAN_CP" \
    -Dsandbox=false \
    -Duse_separate_classloader=false \
    -Dtest_dir="$TEST_DIR" \
    -Dreport_dir="$REPORT_DIR"
done

echo "========================================================="
echo " Generation Complete!"
echo " Tests exported to: $TEST_DIR"
echo " Reports saved to:  $REPORT_DIR"
echo "========================================================="