/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.expressions;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.UUID;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestExpressionParserWithResolvedReference {

  private static final Types.StructType STRUCT_TYPE =
      Types.StructType.of(
          required(100, "id", Types.LongType.get()),
          optional(101, "data", Types.StringType.get()),
          required(102, "b", Types.BooleanType.get()),
          optional(103, "i", Types.IntegerType.get()),
          required(104, "l", Types.LongType.get()),
          optional(105, "f", Types.FloatType.get()),
          required(106, "d", Types.DoubleType.get()),
          optional(107, "date", Types.DateType.get()),
          required(108, "ts", Types.TimestampType.withoutZone()),
          required(110, "s", Types.StringType.get()),
          required(111, "uuid", Types.UUIDType.get()),
          required(112, "fixed", Types.FixedType.ofLength(7)),
          optional(113, "bytes", Types.BinaryType.get()),
          required(114, "dec_9_0", Types.DecimalType.of(9, 0)),
          required(115, "dec_11_2", Types.DecimalType.of(11, 2)));

  private static final Schema SCHEMA = new Schema(STRUCT_TYPE.fields());

  @Test
  public void testResolvedReferenceExpressionSerialization() {
    // Create expressions using ResolvedReference
    Expression[] resolvedExpressions = new Expression[] {
      Expressions.equal(Expressions.ref("id", 100), 42L),
      Expressions.lessThan(Expressions.ref("data", 101), "test"),
      Expressions.greaterThanOrEqual(Expressions.ref("i", 103), 10),
      Expressions.isNull(Expressions.ref("f", 105)),
      Expressions.notNull(Expressions.ref("date", 107)),
      Expressions.startsWith(Expressions.ref("s", 110), "prefix"),
      Expressions.in(Expressions.ref("l", 104), 1L, 2L, 3L),
      Expressions.notIn(Expressions.ref("b", 102), true, false),
      Expressions.isNaN(Expressions.ref("d", 106)),
      Expressions.notNaN(Expressions.ref("f", 105))
    };

    for (Expression expr : resolvedExpressions) {
      // Verify the expression uses ResolvedReference
      assertThat(expr).isInstanceOf(UnboundPredicate.class);
      UnboundPredicate<?> predicate = (UnboundPredicate<?>) expr;
      assertThat(predicate.term()).isInstanceOf(ResolvedReference.class);
      
      // Test JSON serialization
      String json = ExpressionParser.toJson(expr, true);
      assertThat(json).isNotNull();
      assertThat(json).contains("\"type\"");
      
      // Test that JSON contains the field name (not field ID since parser doesn't support it yet)
      ResolvedReference<?> resolvedRef = (ResolvedReference<?>) predicate.term();
      assertThat(json).contains(resolvedRef.name());
    }
  }

  @Test
  public void testResolvedReferenceRoundTripCompatibility() {
    // Test that ResolvedReference expressions can be serialized and parsed back
    Expression resolvedExpr = Expressions.equal(Expressions.ref("id", 100), 42L);
    Expression namedExpr = Expressions.equal(Expressions.ref("id"), 42L);
    
    // Both should produce the same JSON since parser only uses names
    String resolvedJson = ExpressionParser.toJson(resolvedExpr, true);
    String namedJson = ExpressionParser.toJson(namedExpr, true);
    assertThat(resolvedJson).isEqualTo(namedJson);
    
    // Parse back and verify equivalence
    Expression parsedFromResolved = ExpressionParser.fromJson(resolvedJson, SCHEMA);
    Expression parsedFromNamed = ExpressionParser.fromJson(namedJson, SCHEMA);
    
    // Both parsed expressions should be equivalent
    assertThat(ExpressionUtil.equivalent(parsedFromResolved, parsedFromNamed, STRUCT_TYPE, true))
        .isTrue();
        
    // The parsed expression should be equivalent to the original named reference expression
    assertThat(ExpressionUtil.equivalent(namedExpr, parsedFromResolved, STRUCT_TYPE, true))
        .isTrue();
  }

  @Test
  public void testResolvedReferenceComplexExpressions() {
    // Test complex expressions with ResolvedReference
    Expression complexExpr = Expressions.and(
        Expressions.or(
            Expressions.equal(Expressions.ref("data", 101), "test"),
            Expressions.isNull(Expressions.ref("data", 101))),
        Expressions.greaterThanOrEqual(Expressions.ref("id", 100), 100L));
    
    // Serialize to JSON
    String json = ExpressionParser.toJson(complexExpr, true);
    assertThat(json).contains("\"type\" : \"and\"");
    assertThat(json).contains("\"type\" : \"or\"");
    assertThat(json).contains("\"data\"");
    assertThat(json).contains("\"id\"");
    
    // Parse back
    Expression parsed = ExpressionParser.fromJson(json, SCHEMA);
    
    // Create equivalent expression with NamedReference for comparison
    Expression namedEquivalent = Expressions.and(
        Expressions.or(
            Expressions.equal("data", "test"),
            Expressions.isNull("data")),
        Expressions.greaterThanOrEqual("id", 100L));
    
    // Should be equivalent
    assertThat(ExpressionUtil.equivalent(namedEquivalent, parsed, STRUCT_TYPE, true))
        .isTrue();
  }

  @Test
  public void testResolvedReferenceTransformExpressions() {
    // Test transform expressions - using NamedReference for transforms since 
    // ResolvedReference needs proper type parameters for transform methods
    Expression dayTransform = Expressions.equal(
        Expressions.day("date"), "2023-01-15");
    Expression bucketTransform = Expressions.equal(
        Expressions.bucket("id", 10), 5);
    
    // Test serialization
    String dayJson = ExpressionParser.toJson(dayTransform, true);
    String bucketJson = ExpressionParser.toJson(bucketTransform, true);
    
    assertThat(dayJson).contains("\"transform\" : \"day\"");
    assertThat(dayJson).contains("\"term\" : \"date\"");
    assertThat(bucketJson).contains("\"transform\" : \"bucket[10]\"");
    assertThat(bucketJson).contains("\"term\" : \"id\"");
    
    // Test round-trip
    Expression parsedDay = ExpressionParser.fromJson(dayJson, SCHEMA);
    Expression parsedBucket = ExpressionParser.fromJson(bucketJson, SCHEMA);
    
    // Should maintain equivalence after round-trip
    assertThat(ExpressionUtil.equivalent(dayTransform, parsedDay, STRUCT_TYPE, true))
        .isTrue();
    assertThat(ExpressionUtil.equivalent(bucketTransform, parsedBucket, STRUCT_TYPE, true))
        .isTrue();
  }

  @Test
  public void testResolvedReferenceBindingAfterParsing() {
    // Test that expressions with ResolvedReference bind correctly after parsing
    Expression original = Expressions.equal(Expressions.ref("id", 100), 42L);
    
    // Serialize and parse
    String json = ExpressionParser.toJson(original, true);
    Expression parsed = ExpressionParser.fromJson(json, SCHEMA);
    
    // Both should bind successfully
    Expression originalBound = Binder.bind(STRUCT_TYPE, original, true);
    Expression parsedBound = Binder.bind(STRUCT_TYPE, parsed, true);
    
    // Both bound expressions should be identical
    assertThat(originalBound).isInstanceOf(BoundPredicate.class);
    assertThat(parsedBound).isInstanceOf(BoundPredicate.class);
    
    BoundPredicate<?> originalBoundPred = (BoundPredicate<?>) originalBound;
    BoundPredicate<?> parsedBoundPred = (BoundPredicate<?>) parsedBound;
    
    // Should reference the same field
    assertThat(originalBoundPred.ref().fieldId()).isEqualTo(100);
    assertThat(parsedBoundPred.ref().fieldId()).isEqualTo(100);
    assertThat(originalBoundPred.ref().name()).isEqualTo("id");
    assertThat(parsedBoundPred.ref().name()).isEqualTo("id");
  }

  @Test
  public void testResolvedReferenceWithDifferentTypes() {
    // Test ResolvedReference with various data types
    Expression[] typedExpressions = new Expression[] {
      Expressions.equal(Expressions.ref("b", 102), true),
      Expressions.equal(Expressions.ref("i", 103), 42),
      Expressions.equal(Expressions.ref("l", 104), 42L),
      Expressions.equal(Expressions.ref("f", 105), 3.14f),
      Expressions.equal(Expressions.ref("d", 106), 3.14159),
      Expressions.equal(Expressions.ref("s", 110), "test string"),
      Expressions.equal(Expressions.ref("uuid", 111), UUID.randomUUID()),
      Expressions.equal(Expressions.ref("dec_11_2", 115), new BigDecimal("123.45"))
    };
    
    for (Expression expr : typedExpressions) {
      // Test serialization doesn't break with different types
      String json = ExpressionParser.toJson(expr, true);
      assertThat(json).isNotNull();
      
      // Test parsing back
      Expression parsed = ExpressionParser.fromJson(json, SCHEMA);
      assertThat(parsed).isNotNull();
      
      // Test binding
      Expression bound = Binder.bind(STRUCT_TYPE, parsed, true);
      assertThat(bound).isInstanceOf(BoundPredicate.class);
    }
  }

  @Test
  public void testResolvedReferenceJsonStructure() {
    // Test the exact JSON structure produced by ResolvedReference
    Expression expr = Expressions.equal(Expressions.ref("data", 101), "test");
    String json = ExpressionParser.toJson(expr, true);
    
    // The JSON should look like a regular reference since parser doesn't support field IDs yet
    String expectedStructure = "{\n" +
        "  \"type\" : \"eq\",\n" +
        "  \"term\" : \"data\",\n" +
        "  \"value\" : \"test\"\n" +
        "}";
    
    assertThat(json).isEqualTo(expectedStructure);
    
    // Verify it parses back correctly
    Expression parsed = ExpressionParser.fromJson(json);
    assertThat(parsed).isInstanceOf(UnboundPredicate.class);
    
    UnboundPredicate<?> predicate = (UnboundPredicate<?>) parsed;
    assertThat(predicate.term()).isInstanceOf(NamedReference.class);
    assertThat(predicate.term().ref().name()).isEqualTo("data");
  }

  @Test
  public void testResolvedReferenceEquivalenceAfterSerialization() {
    // Test that ResolvedReference expressions maintain equivalence after serialization
    Expression resolvedExpr = Expressions.and(
        Expressions.greaterThan(Expressions.ref("id", 100), 50L),
        Expressions.lessThan(Expressions.ref("id", 100), 200L));
        
    Expression namedExpr = Expressions.and(
        Expressions.greaterThan("id", 50L),
        Expressions.lessThan("id", 200L));
    
    // Serialize both
    String resolvedJson = ExpressionParser.toJson(resolvedExpr, true);
    String namedJson = ExpressionParser.toJson(namedExpr, true);
    
    // Should produce identical JSON
    assertThat(resolvedJson).isEqualTo(namedJson);
    
    // Parse both back
    Expression parsedResolved = ExpressionParser.fromJson(resolvedJson, SCHEMA);
    Expression parsedNamed = ExpressionParser.fromJson(namedJson, SCHEMA);
    
    // All should be equivalent
    assertThat(ExpressionUtil.equivalent(resolvedExpr, namedExpr, STRUCT_TYPE, true))
        .isTrue();
    assertThat(ExpressionUtil.equivalent(parsedResolved, parsedNamed, STRUCT_TYPE, true))
        .isTrue();
    assertThat(ExpressionUtil.equivalent(resolvedExpr, parsedResolved, STRUCT_TYPE, true))
        .isTrue();
  }

  @Test
  public void testResolvedReferenceTransformExpressionEquivalence() {
    // Test expressions that reference transforms where the transform terms are equivalent to resolved references
    // Since UnboundTransform only accepts NamedReference, we test equivalence through binding/unbinding
    
    // Create transform expressions using NamedReference (current approach)
    Expression dayTransformNamed = Expressions.equal(Expressions.day("date"), "2023-01-15");
    Expression bucketTransformNamed = Expressions.equal(Expressions.bucket("id", 10), 5);
    Expression truncateTransformNamed = Expressions.equal(Expressions.truncate("data", 4), "test");
    
    // Create equivalent expressions using ResolvedReference for the predicate terms
    Expression dayWithResolvedRef = Expressions.equal(Expressions.ref("date", 107), "2023-01-15");
    Expression bucketWithResolvedRef = Expressions.equal(Expressions.ref("id", 100), 5L);
    Expression truncateWithResolvedRef = Expressions.equal(Expressions.ref("data", 101), "test");
    
    // Bind all expressions
    Expression boundDayTransform = Binder.bind(STRUCT_TYPE, dayTransformNamed, true);
    Expression boundBucketTransform = Binder.bind(STRUCT_TYPE, bucketTransformNamed, true);
    Expression boundTruncateTransform = Binder.bind(STRUCT_TYPE, truncateTransformNamed, true);
    
    Expression boundDayResolved = Binder.bind(STRUCT_TYPE, dayWithResolvedRef, true);
    Expression boundBucketResolved = Binder.bind(STRUCT_TYPE, bucketWithResolvedRef, true);
    Expression boundTruncateResolved = Binder.bind(STRUCT_TYPE, truncateWithResolvedRef, true);
    
    // Verify all expressions bound successfully
    assertThat(boundDayTransform).isInstanceOf(BoundPredicate.class);
    assertThat(boundBucketTransform).isInstanceOf(BoundPredicate.class);
    assertThat(boundTruncateTransform).isInstanceOf(BoundPredicate.class);
    assertThat(boundDayResolved).isInstanceOf(BoundPredicate.class);
    assertThat(boundBucketResolved).isInstanceOf(BoundPredicate.class);
    assertThat(boundTruncateResolved).isInstanceOf(BoundPredicate.class);
    
    // Test transform expressions in complex expressions with resolved references
    Expression complexTransformExpr = Expressions.and(
        Expressions.equal(Expressions.day("date"), "2023-01-15"),
        Expressions.equal(Expressions.ref("id", 100), 42L));
        
    Expression boundComplexExpr = Binder.bind(STRUCT_TYPE, complexTransformExpr, true);
    assertThat(boundComplexExpr).isInstanceOf(And.class);
    
    // Verify serialization works for transform expressions that coexist with resolved references
    String complexJson = ExpressionParser.toJson(complexTransformExpr, true);
    assertThat(complexJson).contains("\"transform\" : \"day\"");
    assertThat(complexJson).contains("\"term\" : \"date\"");
    assertThat(complexJson).contains("\"term\" : \"id\"");
    
    // Verify round-trip maintains correctness
    Expression parsedComplex = ExpressionParser.fromJson(complexJson, SCHEMA);
    Expression boundParsedComplex = Binder.bind(STRUCT_TYPE, parsedComplex, true);
    
    // Both bound expressions should reference the same fields
    assertThat(boundComplexExpr.toString()).isEqualTo(boundParsedComplex.toString());
  }

  @Test 
  public void testResolvedReferenceInComplexTransformExpressions() {
    // Test complex expressions that combine transforms with resolved references
    Expression complexExpr = Expressions.or(
        Expressions.and(
            Expressions.equal(Expressions.bucket("id", 8), 3),
            Expressions.equal(Expressions.ref("data", 101), "test")),
        Expressions.and(
            Expressions.equal(Expressions.day("date"), "2023-01-15"), 
            Expressions.isNull(Expressions.ref("f", 105))));
    
    // Test serialization
    String json = ExpressionParser.toJson(complexExpr, true);
    assertThat(json).contains("\"transform\" : \"bucket[8]\"");
    assertThat(json).contains("\"transform\" : \"day\"");
    assertThat(json).contains("\"term\" : \"id\"");
    assertThat(json).contains("\"term\" : \"date\"");
    assertThat(json).contains("\"term\" : \"data\"");
    assertThat(json).contains("\"term\" : \"f\"");
    
    // Test that parsing back maintains structure
    Expression parsed = ExpressionParser.fromJson(json, SCHEMA);
    assertThat(parsed).isInstanceOf(Or.class);
    
    // Test binding works correctly
    Expression bound = Binder.bind(STRUCT_TYPE, parsed, true);
    assertThat(bound).isInstanceOf(Or.class);
    
    // Verify equivalence with original
    assertThat(ExpressionUtil.equivalent(complexExpr, parsed, STRUCT_TYPE, true)).isTrue();
    
    // Test that mixed transform and resolved reference expressions bind to same fields
    Expression originalBound = Binder.bind(STRUCT_TYPE, complexExpr, true);
    
    // Both bound expressions should be structurally equivalent
    assertThat(originalBound.toString()).isEqualTo(bound.toString());
  }

  @Test
  public void testTransformExpressionsWithResolvedReference() {
    // Test expressions that reference transforms which in turn reference resolved references
    // Create transforms using the new ResolvedReference-based factory methods
    Expression bucketExpr = Expressions.equal(
        Expressions.bucket(Expressions.ref("id", 100), 8), 3);
    Expression dayExpr = Expressions.equal(
        Expressions.day(Expressions.ref("date", 107)), "2023-01-15");
    Expression hourExpr = Expressions.equal(
        Expressions.hour(Expressions.ref("ts", 108)), 10);
    Expression truncateExpr = Expressions.equal(
        Expressions.truncate(Expressions.ref("data", 101), 4), "test");
    
    // Verify the expressions are created correctly
    assertThat(bucketExpr).isInstanceOf(UnboundPredicate.class);
    assertThat(dayExpr).isInstanceOf(UnboundPredicate.class);
    assertThat(hourExpr).isInstanceOf(UnboundPredicate.class);
    assertThat(truncateExpr).isInstanceOf(UnboundPredicate.class);
    
    // Verify the terms are ResolvedTransform instances
    UnboundPredicate<?> bucketPred = (UnboundPredicate<?>) bucketExpr;
    UnboundPredicate<?> dayPred = (UnboundPredicate<?>) dayExpr;
    UnboundPredicate<?> hourPred = (UnboundPredicate<?>) hourExpr;
    UnboundPredicate<?> truncatePred = (UnboundPredicate<?>) truncateExpr;
    
    assertThat(bucketPred.term()).isInstanceOf(ResolvedTransform.class);
    assertThat(dayPred.term()).isInstanceOf(ResolvedTransform.class);
    assertThat(hourPred.term()).isInstanceOf(ResolvedTransform.class);
    assertThat(truncatePred.term()).isInstanceOf(ResolvedTransform.class);
    
    // Verify that ResolvedTransform preserves the ResolvedReference with field IDs
    ResolvedTransform<?, ?> bucketTransform = (ResolvedTransform<?, ?>) bucketPred.term();
    ResolvedTransform<?, ?> dayTransform = (ResolvedTransform<?, ?>) dayPred.term();
    ResolvedTransform<?, ?> hourTransform = (ResolvedTransform<?, ?>) hourPred.term();
    ResolvedTransform<?, ?> truncateTransform = (ResolvedTransform<?, ?>) truncatePred.term();
    
    assertThat(bucketTransform.resolvedRef().fieldId()).isEqualTo(100);
    assertThat(bucketTransform.resolvedRef().name()).isEqualTo("id");
    assertThat(dayTransform.resolvedRef().fieldId()).isEqualTo(107);
    assertThat(dayTransform.resolvedRef().name()).isEqualTo("date");
    assertThat(hourTransform.resolvedRef().fieldId()).isEqualTo(108);
    assertThat(hourTransform.resolvedRef().name()).isEqualTo("ts");
    assertThat(truncateTransform.resolvedRef().fieldId()).isEqualTo(101);
    assertThat(truncateTransform.resolvedRef().name()).isEqualTo("data");
    
    // Test serialization
    String bucketJson = ExpressionParser.toJson(bucketExpr, true);
    String dayJson = ExpressionParser.toJson(dayExpr, true);
    String hourJson = ExpressionParser.toJson(hourExpr, true);
    String truncateJson = ExpressionParser.toJson(truncateExpr, true);
    
    // Verify JSON contains the expected transform and term information
    assertThat(bucketJson).contains("\"transform\" : \"bucket[8]\"");
    assertThat(bucketJson).contains("\"term\" : \"id\"");
    assertThat(dayJson).contains("\"transform\" : \"day\"");
    assertThat(dayJson).contains("\"term\" : \"date\"");
    assertThat(hourJson).contains("\"transform\" : \"hour\"");
    assertThat(hourJson).contains("\"term\" : \"ts\"");
    assertThat(truncateJson).contains("\"transform\" : \"truncate[4]\"");
    assertThat(truncateJson).contains("\"term\" : \"data\"");
    
    // Test parsing back
    Expression parsedBucket = ExpressionParser.fromJson(bucketJson, SCHEMA);
    Expression parsedDay = ExpressionParser.fromJson(dayJson, SCHEMA);
    Expression parsedHour = ExpressionParser.fromJson(hourJson, SCHEMA);
    Expression parsedTruncate = ExpressionParser.fromJson(truncateJson, SCHEMA);
    
    // Verify equivalence after round-trip
    assertThat(ExpressionUtil.equivalent(bucketExpr, parsedBucket, STRUCT_TYPE, true)).isTrue();
    assertThat(ExpressionUtil.equivalent(dayExpr, parsedDay, STRUCT_TYPE, true)).isTrue();
    assertThat(ExpressionUtil.equivalent(hourExpr, parsedHour, STRUCT_TYPE, true)).isTrue();
    assertThat(ExpressionUtil.equivalent(truncateExpr, parsedTruncate, STRUCT_TYPE, true)).isTrue();
    
    // Test binding works correctly
    Expression boundBucket = Binder.bind(STRUCT_TYPE, parsedBucket, true);
    Expression boundDay = Binder.bind(STRUCT_TYPE, parsedDay, true);
    Expression boundHour = Binder.bind(STRUCT_TYPE, parsedHour, true);
    Expression boundTruncate = Binder.bind(STRUCT_TYPE, parsedTruncate, true);
    
    assertThat(boundBucket).isInstanceOf(BoundPredicate.class);
    assertThat(boundDay).isInstanceOf(BoundPredicate.class);
    assertThat(boundHour).isInstanceOf(BoundPredicate.class);
    assertThat(boundTruncate).isInstanceOf(BoundPredicate.class);
    
    // Verify bound expressions reference correct field IDs
    BoundPredicate<?> boundBucketPred = (BoundPredicate<?>) boundBucket;
    BoundPredicate<?> boundDayPred = (BoundPredicate<?>) boundDay;
    BoundPredicate<?> boundHourPred = (BoundPredicate<?>) boundHour;
    BoundPredicate<?> boundTruncatePred = (BoundPredicate<?>) boundTruncate;
    
    assertThat(boundBucketPred.term()).isInstanceOf(BoundTransform.class);
    assertThat(boundDayPred.term()).isInstanceOf(BoundTransform.class);
    assertThat(boundHourPred.term()).isInstanceOf(BoundTransform.class);
    assertThat(boundTruncatePred.term()).isInstanceOf(BoundTransform.class);
  }

  @Test
  public void testComplexExpressionsWithResolvedReferenceTransforms() {
    // Test complex expressions combining transforms created from ResolvedReference
    Expression complexExpr = Expressions.and(
        Expressions.or(
            Expressions.equal(Expressions.bucket(Expressions.ref("id", 100), 8), 3),
            Expressions.equal(Expressions.day(Expressions.ref("date", 107)), "2023-01-15")),
        Expressions.and(
            Expressions.equal(Expressions.truncate(Expressions.ref("data", 101), 4), "test"),
            Expressions.isNull(Expressions.ref("f", 105))));
    
    // Test serialization of complex expression
    String json = ExpressionParser.toJson(complexExpr, true);
    
    // Verify all transforms and terms are present in JSON
    assertThat(json).contains("\"transform\" : \"bucket[8]\"");
    assertThat(json).contains("\"transform\" : \"day\"");
    assertThat(json).contains("\"transform\" : \"truncate[4]\"");
    assertThat(json).contains("\"term\" : \"id\"");
    assertThat(json).contains("\"term\" : \"date\"");
    assertThat(json).contains("\"term\" : \"data\"");
    assertThat(json).contains("\"term\" : \"f\"");
    
    // Test parsing back maintains structure
    Expression parsed = ExpressionParser.fromJson(json, SCHEMA);
    assertThat(parsed).isInstanceOf(And.class);
    
    // Test binding works correctly for the complex expression
    Expression bound = Binder.bind(STRUCT_TYPE, parsed, true);
    assertThat(bound).isInstanceOf(And.class);
    
    // Verify equivalence after round-trip
    assertThat(ExpressionUtil.equivalent(complexExpr, parsed, STRUCT_TYPE, true)).isTrue();
    
    // Test that the bound expression maintains correct structure
    Expression originalBound = Binder.bind(STRUCT_TYPE, complexExpr, true);
    assertThat(originalBound.toString()).isEqualTo(bound.toString());
  }

  @Test
  public void testResolvedReferenceTransformCompatibilityWithNamedReference() {
    // Test that transforms created with ResolvedReference are equivalent to those created with NamedReference
    
    // Create equivalent transforms using both approaches
    Expression bucketWithResolved = Expressions.equal(
        Expressions.bucket(Expressions.ref("id", 100), 8), 3);
    Expression bucketWithNamed = Expressions.equal(
        Expressions.bucket("id", 8), 3);
    
    Expression dayWithResolved = Expressions.equal(
        Expressions.day(Expressions.ref("date", 107)), "2023-01-15");
    Expression dayWithNamed = Expressions.equal(
        Expressions.day("date"), "2023-01-15");
    
    // Test that both serialize to the same JSON
    String resolvedBucketJson = ExpressionParser.toJson(bucketWithResolved, true);
    String namedBucketJson = ExpressionParser.toJson(bucketWithNamed, true);
    String resolvedDayJson = ExpressionParser.toJson(dayWithResolved, true);
    String namedDayJson = ExpressionParser.toJson(dayWithNamed, true);
    
    assertThat(resolvedBucketJson).isEqualTo(namedBucketJson);
    assertThat(resolvedDayJson).isEqualTo(namedDayJson);
    
    // Test that parsing back produces equivalent expressions
    Expression parsedResolvedBucket = ExpressionParser.fromJson(resolvedBucketJson, SCHEMA);
    Expression parsedNamedBucket = ExpressionParser.fromJson(namedBucketJson, SCHEMA);
    Expression parsedResolvedDay = ExpressionParser.fromJson(resolvedDayJson, SCHEMA);
    Expression parsedNamedDay = ExpressionParser.fromJson(namedDayJson, SCHEMA);
    
    // All should be equivalent
    assertThat(ExpressionUtil.equivalent(bucketWithResolved, bucketWithNamed, STRUCT_TYPE, true)).isTrue();
    assertThat(ExpressionUtil.equivalent(dayWithResolved, dayWithNamed, STRUCT_TYPE, true)).isTrue();
    assertThat(ExpressionUtil.equivalent(parsedResolvedBucket, parsedNamedBucket, STRUCT_TYPE, true)).isTrue();
    assertThat(ExpressionUtil.equivalent(parsedResolvedDay, parsedNamedDay, STRUCT_TYPE, true)).isTrue();
    
    // Test that binding produces identical results
    Expression boundResolvedBucket = Binder.bind(STRUCT_TYPE, parsedResolvedBucket, true);
    Expression boundNamedBucket = Binder.bind(STRUCT_TYPE, parsedNamedBucket, true);
    Expression boundResolvedDay = Binder.bind(STRUCT_TYPE, parsedResolvedDay, true);
    Expression boundNamedDay = Binder.bind(STRUCT_TYPE, parsedNamedDay, true);
    
    assertThat(boundResolvedBucket.toString()).isEqualTo(boundNamedBucket.toString());
    assertThat(boundResolvedDay.toString()).isEqualTo(boundNamedDay.toString());
  }

  @Test
  public void testResolvedTransformPreservesFieldIdInformation() {
    // Test that ResolvedTransform preserves field ID information through binding
    // This is the key advantage over UnboundTransform with NamedReference
    
    // Create a transform expression using ResolvedReference
    ResolvedReference<Long> idRef = Expressions.ref("id", 100);
    Expression bucketExpr = Expressions.equal(Expressions.bucket(idRef, 8), 3);
    
    // Verify it's a ResolvedTransform
    UnboundPredicate<?> predicate = (UnboundPredicate<?>) bucketExpr;
    assertThat(predicate.term()).isInstanceOf(ResolvedTransform.class);
    
    ResolvedTransform<?, ?> resolvedTransform = (ResolvedTransform<?, ?>) predicate.term();
    
    // Verify the ResolvedReference is preserved with field ID
    assertThat(resolvedTransform.resolvedRef().fieldId()).isEqualTo(100);
    assertThat(resolvedTransform.resolvedRef().name()).isEqualTo("id");
    
    // Test binding works correctly with field ID information
    Expression bound = Binder.bind(STRUCT_TYPE, bucketExpr, true);
    assertThat(bound).isInstanceOf(BoundPredicate.class);
    
    BoundPredicate<?> boundPredicate = (BoundPredicate<?>) bound;
    assertThat(boundPredicate.term()).isInstanceOf(BoundTransform.class);
    
    BoundTransform<?, ?> boundTransform = (BoundTransform<?, ?>) boundPredicate.term();
    assertThat(boundTransform.ref().fieldId()).isEqualTo(100);
    assertThat(boundTransform.ref().name()).isEqualTo("id");
    
    // Compare with equivalent NamedReference approach to show they bind to same field
    Expression namedBucketExpr = Expressions.equal(Expressions.bucket("id", 8), 3);
    Expression boundNamed = Binder.bind(STRUCT_TYPE, namedBucketExpr, true);
    
    // Both should resolve to the same field ID since they reference the same field
    BoundPredicate<?> boundNamedPredicate = (BoundPredicate<?>) boundNamed;
    BoundTransform<?, ?> boundNamedTransform = (BoundTransform<?, ?>) boundNamedPredicate.term();
    
    assertThat(boundTransform.ref().fieldId()).isEqualTo(boundNamedTransform.ref().fieldId());
    assertThat(boundTransform.toString()).isEqualTo(boundNamedTransform.toString());
  }

  @Test
  public void testResolvedTransformExpressionParserIntegration() {
    // Test that expressions with ResolvedTransform integrate correctly with ExpressionParser
    
    // Create expressions using ResolvedTransform
    Expression bucketExpr = Expressions.equal(
        Expressions.bucket(Expressions.ref("id", 100), 8), 3);
    Expression dayExpr = Expressions.equal(
        Expressions.day(Expressions.ref("date", 107)), "2023-01-15");
    
    // Test that they can be serialized (even though they become NamedReference in JSON)
    String bucketJson = ExpressionParser.toJson(bucketExpr, true);
    String dayJson = ExpressionParser.toJson(dayExpr, true);
    
    assertThat(bucketJson).contains("\"transform\" : \"bucket[8]\"");
    assertThat(bucketJson).contains("\"term\" : \"id\"");
    assertThat(dayJson).contains("\"transform\" : \"day\"");
    assertThat(dayJson).contains("\"term\" : \"date\"");
    
    // Test parsing back (will create UnboundTransform with NamedReference)
    Expression parsedBucket = ExpressionParser.fromJson(bucketJson, SCHEMA);
    Expression parsedDay = ExpressionParser.fromJson(dayJson, SCHEMA);
    
    // The parsed expressions will have UnboundTransform (not ResolvedTransform)
    // but they should still be functionally equivalent when bound
    UnboundPredicate<?> parsedBucketPred = (UnboundPredicate<?>) parsedBucket;
    UnboundPredicate<?> parsedDayPred = (UnboundPredicate<?>) parsedDay;
    
    assertThat(parsedBucketPred.term()).isInstanceOf(UnboundTransform.class);
    assertThat(parsedDayPred.term()).isInstanceOf(UnboundTransform.class);
    
    // Both original and parsed should bind to the same fields
    Expression originalBucketBound = Binder.bind(STRUCT_TYPE, bucketExpr, true);
    Expression parsedBucketBound = Binder.bind(STRUCT_TYPE, parsedBucket, true);
    Expression originalDayBound = Binder.bind(STRUCT_TYPE, dayExpr, true);
    Expression parsedDayBound = Binder.bind(STRUCT_TYPE, parsedDay, true);
    
    assertThat(originalBucketBound.toString()).isEqualTo(parsedBucketBound.toString());
    assertThat(originalDayBound.toString()).isEqualTo(parsedDayBound.toString());
    
    // Test equivalence
    assertThat(ExpressionUtil.equivalent(bucketExpr, parsedBucket, STRUCT_TYPE, true)).isTrue();
    assertThat(ExpressionUtil.equivalent(dayExpr, parsedDay, STRUCT_TYPE, true)).isTrue();
  }

  @Test
  public void testMixedResolvedTransformAndResolvedReferenceExpressions() {
    // Test complex expressions mixing ResolvedTransform and direct ResolvedReference
    Expression complexExpr = Expressions.and(
        Expressions.equal(Expressions.bucket(Expressions.ref("id", 100), 8), 3),
        Expressions.or(
            Expressions.equal(Expressions.ref("data", 101), "test"),
            Expressions.equal(Expressions.day(Expressions.ref("date", 107)), "2023-01-15")));
    
    // Verify structure contains both ResolvedTransform and ResolvedReference
    assertThat(complexExpr).isInstanceOf(And.class);
    And andExpr = (And) complexExpr;
    
    // First part should be ResolvedTransform
    UnboundPredicate<?> bucketPred = (UnboundPredicate<?>) andExpr.left();
    assertThat(bucketPred.term()).isInstanceOf(ResolvedTransform.class);
    
    // Second part is OR with ResolvedReference and ResolvedTransform
    Or orExpr = (Or) andExpr.right();
    UnboundPredicate<?> dataPred = (UnboundPredicate<?>) orExpr.left();
    UnboundPredicate<?> dayPred = (UnboundPredicate<?>) orExpr.right();
    
    assertThat(dataPred.term()).isInstanceOf(ResolvedReference.class);
    assertThat(dayPred.term()).isInstanceOf(ResolvedTransform.class);
    
    // Test serialization and parsing
    String json = ExpressionParser.toJson(complexExpr, true);
    Expression parsed = ExpressionParser.fromJson(json, SCHEMA);
    
    // Test binding works correctly
    Expression bound = Binder.bind(STRUCT_TYPE, parsed, true);
    Expression originalBound = Binder.bind(STRUCT_TYPE, complexExpr, true);
    
    assertThat(bound.toString()).isEqualTo(originalBound.toString());
    assertThat(ExpressionUtil.equivalent(complexExpr, parsed, STRUCT_TYPE, true)).isTrue();
  }

  @Test
  public void testBoundExpressionSerializationWithResolvedReference() {
    // Test that bound expressions serialize to JSON with ResolvedReference terms when includeFieldIds=true
    
    // Create and bind simple expressions
    Expression simpleExpr = Expressions.equal(Expressions.ref("id", 100), 42L);
    Expression boundSimple = Binder.bind(STRUCT_TYPE, simpleExpr, true);
    
    // Test serialization without field IDs (existing behavior)
    String jsonWithoutFieldIds = ExpressionParser.toJson(boundSimple, true);
    assertThat(jsonWithoutFieldIds).contains("\"term\" : \"id\"");
    assertThat(jsonWithoutFieldIds).doesNotContain("fieldId");
    
    // Test serialization with field IDs (new behavior)
    String jsonWithFieldIds = ExpressionParser.toJson(boundSimple, true, true);
    assertThat(jsonWithFieldIds).contains("\"type\" : \"ref\"");
    assertThat(jsonWithFieldIds).contains("\"name\" : \"id\"");
    assertThat(jsonWithFieldIds).contains("\"fieldId\" : 100");
    
    // Test complex expressions
    Expression complexExpr = Expressions.and(
        Expressions.equal(Expressions.ref("data", 101), "test"),
        Expressions.greaterThan(Expressions.ref("id", 100), 50L));
    Expression boundComplex = Binder.bind(STRUCT_TYPE, complexExpr, true);
    
    String complexJsonWithFieldIds = ExpressionParser.toJson(boundComplex, true, true);
    assertThat(complexJsonWithFieldIds).contains("\"fieldId\" : 100");
    assertThat(complexJsonWithFieldIds).contains("\"fieldId\" : 101");
    assertThat(complexJsonWithFieldIds).contains("\"name\" : \"id\"");
    assertThat(complexJsonWithFieldIds).contains("\"name\" : \"data\"");
  }

  @Test
  public void testBoundTransformExpressionSerializationWithResolvedReference() {
    // Test that bound transform expressions serialize to JSON with ResolvedReference terms when includeFieldIds=true
    
    // Create transform expressions using ResolvedTransform
    Expression bucketExpr = Expressions.equal(
        Expressions.bucket(Expressions.ref("id", 100), 8), 3);
    Expression dayExpr = Expressions.equal(
        Expressions.day(Expressions.ref("date", 107)), "2023-01-15");
    
    // Bind the expressions
    Expression boundBucket = Binder.bind(STRUCT_TYPE, bucketExpr, true);
    Expression boundDay = Binder.bind(STRUCT_TYPE, dayExpr, true);
    
    // Test serialization without field IDs (existing behavior)
    String bucketJsonNoFieldIds = ExpressionParser.toJson(boundBucket, true);
    assertThat(bucketJsonNoFieldIds).contains("\"transform\" : \"bucket[8]\"");
    assertThat(bucketJsonNoFieldIds).contains("\"term\" : \"id\"");
    assertThat(bucketJsonNoFieldIds).doesNotContain("fieldId");
    
    // Test serialization with field IDs (new behavior)
    String bucketJsonWithFieldIds = ExpressionParser.toJson(boundBucket, true, true);
    assertThat(bucketJsonWithFieldIds).contains("\"transform\" : \"bucket[8]\"");
    assertThat(bucketJsonWithFieldIds).contains("\"type\" : \"ref\"");
    assertThat(bucketJsonWithFieldIds).contains("\"name\" : \"id\"");
    assertThat(bucketJsonWithFieldIds).contains("\"fieldId\" : 100");
    
    String dayJsonWithFieldIds = ExpressionParser.toJson(boundDay, true, true);
    assertThat(dayJsonWithFieldIds).contains("\"transform\" : \"day\"");
    assertThat(dayJsonWithFieldIds).contains("\"type\" : \"ref\"");
    assertThat(dayJsonWithFieldIds).contains("\"name\" : \"date\"");
    assertThat(dayJsonWithFieldIds).contains("\"fieldId\" : 107");
  }

  @Test
  public void testComplexBoundTransformExpressionSerializationWithResolvedReference() {
    // Test complex bound expressions with mixed transforms and references
    Expression complexExpr = Expressions.and(
        Expressions.or(
            Expressions.equal(Expressions.bucket(Expressions.ref("id", 100), 8), 3),
            Expressions.equal(Expressions.day(Expressions.ref("date", 107)), "2023-01-15")),
        Expressions.and(
            Expressions.equal(Expressions.truncate(Expressions.ref("data", 101), 4), "test"),
            Expressions.isNull(Expressions.ref("f", 105))));
    
    // Bind the complex expression
    Expression bound = Binder.bind(STRUCT_TYPE, complexExpr, true);
    
    // Test serialization with field IDs
    String jsonWithFieldIds = ExpressionParser.toJson(bound, true, true);
    
    // Verify all field IDs are present
    assertThat(jsonWithFieldIds).contains("\"fieldId\" : 100"); // id field
    assertThat(jsonWithFieldIds).contains("\"fieldId\" : 107"); // date field  
    assertThat(jsonWithFieldIds).contains("\"fieldId\" : 101"); // data field
    assertThat(jsonWithFieldIds).contains("\"fieldId\" : 105"); // f field
    
    // Verify all field names are present
    assertThat(jsonWithFieldIds).contains("\"name\" : \"id\"");
    assertThat(jsonWithFieldIds).contains("\"name\" : \"date\"");
    assertThat(jsonWithFieldIds).contains("\"name\" : \"data\"");
    assertThat(jsonWithFieldIds).contains("\"name\" : \"f\"");
    
    // Verify transforms are serialized correctly
    assertThat(jsonWithFieldIds).contains("\"transform\" : \"bucket[8]\"");
    assertThat(jsonWithFieldIds).contains("\"transform\" : \"day\"");
    assertThat(jsonWithFieldIds).contains("\"transform\" : \"truncate[4]\"");
    
    // Verify ResolvedReference structure for both transforms and direct references
    // Count occurrences of "type" : "ref" to ensure all references use ResolvedReference format
    long refTypeCount = jsonWithFieldIds.lines()
        .mapToLong(line -> {
          int count = 0;
          int index = 0;
          String pattern = "\"type\" : \"ref\"";
          while ((index = line.indexOf(pattern, index)) != -1) {
            count++;
            index += pattern.length();
          }
          return count;
        })
        .sum();
    
    assertThat(refTypeCount).isEqualTo(4); // Should have 4 ResolvedReference objects
  }

  @Test
  public void testBoundExpressionRoundTripWithResolvedReference() {
    // Test that bound expressions with ResolvedReference can be serialized and maintain information
    
    // Create expressions using both ResolvedTransform and ResolvedReference
    Expression originalExpr = Expressions.and(
        Expressions.equal(Expressions.bucket(Expressions.ref("id", 100), 8), 3),
        Expressions.equal(Expressions.ref("data", 101), "test"));
    
    // Bind the expression
    Expression bound = Binder.bind(STRUCT_TYPE, originalExpr, true);
    
    // Serialize with field IDs
    String jsonWithFieldIds = ExpressionParser.toJson(bound, true, true);
    
    // Verify the JSON structure contains complete ResolvedReference information
    assertThat(jsonWithFieldIds).contains("\"type\" : \"ref\"");
    assertThat(jsonWithFieldIds).contains("\"fieldId\" : 100");
    assertThat(jsonWithFieldIds).contains("\"fieldId\" : 101");
    
    // Note: The current parser doesn't support parsing ResolvedReference format back to expressions
    // This test documents the serialization capability for external systems that need field ID information
    
    // Verify that the bound expression maintains the same field IDs after serialization
    BoundPredicate<?> boundPred = (BoundPredicate<?>) ((And) bound).left();
    BoundTransform<?, ?> boundTransform = (BoundTransform<?, ?>) boundPred.term();
    assertThat(boundTransform.ref().fieldId()).isEqualTo(100);
    
    BoundPredicate<?> boundDataPred = (BoundPredicate<?>) ((And) bound).right();
    BoundReference<?> boundDataRef = (BoundReference<?>) boundDataPred.term();
    assertThat(boundDataRef.fieldId()).isEqualTo(101);
  }

  @Test
  public void testBoundExpressionFieldIdPreservationAcrossAllTypes() {
    // Test that all data types preserve field IDs correctly in bound expression serialization
    
    // Create expressions for fields that support meaningful comparisons
    Expression[] expressions = new Expression[] {
        Expressions.equal(Expressions.ref("id", 100), 42L),
        Expressions.equal(Expressions.ref("data", 101), "test"),
        Expressions.equal(Expressions.ref("b", 102), true),
        Expressions.equal(Expressions.ref("i", 103), 42),
        Expressions.equal(Expressions.ref("l", 104), 42L),
        Expressions.equal(Expressions.ref("f", 105), 3.14f),
        Expressions.equal(Expressions.ref("d", 106), 3.14159),
        Expressions.equal(Expressions.ref("s", 110), "test-string"),
        Expressions.greaterThan(Expressions.ref("id", 100), 10L)
    };
    
    for (Expression expr : expressions) {
      // Bind each expression
      Expression bound = Binder.bind(STRUCT_TYPE, expr, true);
      
      // Serialize with field IDs
      String json = ExpressionParser.toJson(bound, true, true);
      
      // Extract expected field ID from the original ResolvedReference
      UnboundPredicate<?> unboundPred = (UnboundPredicate<?>) expr;
      ResolvedReference<?> resolvedRef = (ResolvedReference<?>) unboundPred.term();
      int expectedFieldId = resolvedRef.fieldId();
      
      // Verify the field ID is preserved in the JSON
      assertThat(json).contains("\"fieldId\" : " + expectedFieldId);
      assertThat(json).contains("\"type\" : \"ref\"");
    }
  }
}