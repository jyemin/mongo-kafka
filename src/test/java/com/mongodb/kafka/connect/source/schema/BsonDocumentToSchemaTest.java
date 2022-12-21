/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.mongodb.kafka.connect.source.schema;

import static com.mongodb.kafka.connect.source.schema.BsonDocumentToSchema.inferDocumentSchema;

import java.util.Arrays;
import java.util.List;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.bson.BsonDocument;

public class BsonDocumentToSchemaTest {

  @Test
  @DisplayName("Schema differ")
  void testDiffSchemas() {
    List<Schema> schemas =
        Arrays.asList(
            inferDocumentSchema(
                BsonDocument.parse(
                    "{\"_id\": \"63a0a1f861152965f5f15b07\", \"advisorReviewRequired\": false, \"createdBy\": {\"firstName\": \"John\", \"lastName\": \"Smith\", \"username\": \"CN999999\"}, \"createdTs\": \"2022-12-19T17:40:08.565Z\", \"isDraft\": false, \"status\": \"\", \"type\": \"determination\", \"updatedBy\": {\"firstName\": \"John\", \"lastName\": \"Smith\", \"username\": \"CN999999\"}, \"updatedTs\": \"2022-12-19T17:40:08.565Z\", \"version\": 0, \"voidInfo\": {\"reason\": {\"name\": \"\", \"code\": \"\", \"description\": \"\"}, \"otherReason\": \"\", \"details\": \"\", \"voidedById\": \"\", \"voidedByName\": \"\", \"ts\": null}, \"peerToPeer\": {\"advisor\": {\"firstName\": \"\", \"lastName\": \"\", \"username\": \"\"}, \"clinician\": {\"firstName\": \"\", \"lastName\": \"\", \"username\": \"\"}, \"provider\": {}, \"contact\": {\"phone\": {\"extension\": \"\", \"number\": \"\"}, \"fax\": {\"extension\": \"\", \"number\": \"\"}}}, \"determination\": {\"_id\": \"63a0a1f861152965f5f15b07\", \"authorization_id\": \"63a0a143044eb7f573d238aa\", \"line\": 1, \"claimsInstruction\": \"denied\", \"completeInd\": true, \"createdBy\": {\"firstName\": \"John\", \"lastName\": \"Smith\", \"username\": \"CN999999\"}, \"createdTs\": \"2022-12-19T17:40:07.974Z\", \"determinedBy\": {\"firstName\": \"John\", \"lastName\": \"Smith\", \"username\": \"CN999999\"}, \"determinationTs\": \"2022-12-19T17:38:38.346Z\", \"explanationCd\": \"EX+W\", \"explanationDesc\": \"AUTH WITHDRAWN BY REQUESTOR\", \"isCurrent\": false, \"isDraft\": false, \"status\": \"DENIAL\", \"typeCd\": \"Appeal\", \"typeDesc\": \"Appeal Determination\", \"updatedBy\": {\"firstName\": \"John\", \"lastName\": \"Smith\", \"username\": \"CN999999\"}, \"updatedTs\": \"2022-12-19T17:40:07.974Z\", \"version\": 1, \"approvedUnitInfo\": {\"fromDate\": null, \"medicalNecessityCd\": \"\", \"medicalNecessityDesc\": \"\", \"toDate\": null, \"units\": 0, \"varianceCd\": \"\", \"varianceDesc\": \"\"}, \"deniedUnitInfo\": {\"fromDate\": \"2022-12-19T00:00:00Z\", \"medicalNecessityCd\": \"Administrative Denial\", \"medicalNecessityDesc\": \"Administrative Denial\", \"toDate\": \"2022-12-22T00:00:00Z\", \"units\": 3, \"varianceCd\": \"72HrReadmit\", \"varianceDesc\": \"72-Hour Readmission\"}, \"pendingUnitInfo\": {\"fromDate\": null, \"medicalNecessityCd\": \"\", \"medicalNecessityDesc\": \"\", \"toDate\": null, \"units\": 0, \"varianceCd\": \"\", \"varianceDesc\": \"\"}}}")),
            inferDocumentSchema(
                BsonDocument.parse(
                    "{\"_id\": \"63a0a2d461152965f5f15b08\", \"advisorReviewRequired\": false, \"createdBy\": {\"firstName\": \"John\", \"lastName\": \"Smith\", \"username\": \"CN999999\"}, \"createdTs\": \"2022-12-19T17:43:48.597Z\", \"isDraft\": false, \"status\": \"\", \"type\": \"determination\", \"updatedBy\": {\"firstName\": \"John\", \"lastName\": \"Smith\", \"username\": \"CN999999\"}, \"updatedTs\": \"2022-12-19T17:43:48.597Z\", \"version\": 0, \"voidInfo\": {\"reason\": {\"name\": \"\", \"code\": \"\", \"description\": \"\"}, \"otherReason\": \"\", \"details\": \"\", \"voidedById\": \"\", \"voidedByName\": \"\", \"ts\": null}, \"peerToPeer\": {\"advisor\": {\"firstName\": \"\", \"lastName\": \"\", \"username\": \"\"}, \"clinician\": {\"firstName\": \"\", \"lastName\": \"\", \"username\": \"\"}, \"provider\": {}, \"contact\": {\"phone\": {\"extension\": \"\", \"number\": \"\"}, \"fax\": {\"extension\": \"\", \"number\": \"\"}}}, \"determination\": {\"_id\": \"63a0a2d461152965f5f15b08\", \"appealId\": \"4N6M-WA3HEW\", \"claimsInstruction\": \"asdasdad\", \"completeInd\": true, \"createdBy\": {\"firstName\": \"\", \"lastName\": \"\", \"username\": \"\"}, \"createdTs\": \"2022-12-19T17:43:31.765Z\", \"determinedBy\": {\"firstName\": \"John\", \"lastName\": \"Smith\", \"username\": \"CN999999\"}, \"determinationTs\": \"2022-12-19T17:43:31.769Z\", \"explanationCd\": \"aR\", \"explanationDesc\": \"Avoidable Readmission\", \"isCurrent\": false, \"isDraft\": false, \"status\": \"APPROVED\", \"typeCd\": \"\", \"typeDesc\": \"\", \"updatedBy\": {\"firstName\": \"\", \"lastName\": \"\", \"username\": \"\"}, \"updatedTs\": \"2022-12-19T17:43:48.31Z\", \"version\": 1, \"approvedUnitInfo\": {\"fromDate\": null, \"medicalNecessityCd\": \"Administrative denial\", \"medicalNecessityDesc\": \"Administrative denial\", \"toDate\": null, \"units\": 3, \"varianceCd\": \"awaiting_guardianship\", \"varianceDesc\": \"Awaiting Guardianship\"}, \"deniedUnitInfo\": {\"fromDate\": null, \"medicalNecessityCd\": \"\", \"medicalNecessityDesc\": \"\", \"toDate\": null, \"units\": 0, \"varianceCd\": \"\", \"varianceDesc\": \"\"}, \"pendingUnitInfo\": {\"fromDate\": null, \"medicalNecessityCd\": \"\", \"medicalNecessityDesc\": \"\", \"toDate\": null, \"units\": 0, \"varianceCd\": \"\", \"varianceDesc\": \"\"}}}")),
            inferDocumentSchema(
                BsonDocument.parse(
                    "{\"_id\": \"63a0cbaa61152965f5f15b0d\", \"advisorReviewRequired\": false, \"createdBy\": {\"firstName\": \"John\", \"lastName\": \"Smith\", \"username\": \"CN999999\"}, \"createdTs\": \"2022-12-19T20:38:02.093Z\", \"isDraft\": false, \"status\": \"\", \"type\": \"determination\", \"updatedBy\": {\"firstName\": \"John\", \"lastName\": \"Smith\", \"username\": \"CN999999\"}, \"updatedTs\": \"2022-12-19T20:38:02.093Z\", \"version\": 0, \"voidInfo\": {\"reason\": {\"name\": \"\", \"code\": \"\", \"description\": \"\"}, \"otherReason\": \"\", \"details\": \"\", \"voidedById\": \"\", \"voidedByName\": \"\", \"ts\": null}, \"determination\": {\"_id\": \"63a0cbaa61152965f5f15b0d\", \"appealId\": \"4N6M-WA3HEW\", \"claimsInstruction\": \"sdf\", \"completeInd\": true, \"createdBy\": {\"firstName\": \"\", \"lastName\": \"\", \"username\": \"\"}, \"createdTs\": \"2022-12-19T20:37:48.7Z\", \"determinedBy\": {\"firstName\": \"John\", \"lastName\": \"Smith\", \"username\": \"CN999999\"}, \"determinationTs\": \"2022-12-19T20:37:48.702Z\", \"explanationCd\": \"aR\", \"explanationDesc\": \"Avoidable Readmission\", \"isDraft\": false, \"status\": \"DENIED\", \"typeCd\": \"\", \"typeDesc\": \"\", \"updatedBy\": {\"firstName\": \"\", \"lastName\": \"\", \"username\": \"\"}, \"updatedTs\": \"2022-12-19T20:38:01.699Z\", \"version\": 1, \"approvedUnitInfo\": {\"fromDate\": null, \"medicalNecessityCd\": \"\", \"medicalNecessityDesc\": \"\", \"toDate\": null, \"units\": 0, \"varianceCd\": \"\", \"varianceDesc\": \"\"}, \"deniedUnitInfo\": {\"fromDate\": null, \"medicalNecessityCd\": \"Not met\", \"medicalNecessityDesc\": \"Not met\", \"toDate\": null, \"units\": 1, \"varianceCd\": \"bh_residential_treatment\", \"varianceDesc\": \"BH Residential Treatment\"}, \"pendingUnitInfo\": {\"fromDate\": null, \"medicalNecessityCd\": \"\", \"medicalNecessityDesc\": \"\", \"toDate\": null, \"units\": 0, \"varianceCd\": \"\", \"varianceDesc\": \"\"}}}")),
            inferDocumentSchema(
                BsonDocument.parse(
                    "{\"_id\": \"63a0cc2d044eb7f573d238e0\", \"advisorReviewRequired\": false, \"createdBy\": {\"firstName\": \"John\", \"lastName\": \"Smith\", \"username\": \"CN999999\"}, \"createdTs\": \"2022-12-19T20:40:13.856Z\", \"isDraft\": false, \"status\": \"\", \"type\": \"determination\", \"updatedBy\": {\"firstName\": \"John\", \"lastName\": \"Smith\", \"username\": \"CN999999\"}, \"updatedTs\": \"2022-12-19T20:40:13.856Z\", \"version\": 0, \"voidInfo\": {\"reason\": {\"name\": \"\", \"code\": \"\", \"description\": \"\"}, \"otherReason\": \"\", \"details\": \"\", \"voidedById\": \"\", \"voidedByName\": \"\", \"ts\": null}, \"determination\": {\"_id\": \"63a0cc2d044eb7f573d238e0\", \"appealId\": \"WHPR-N7F7FM\", \"completeInd\": true, \"createdBy\": {\"firstName\": \"\", \"lastName\": \"\", \"username\": \"\"}, \"createdTs\": \"2022-12-19T20:40:03.206Z\", \"determinedBy\": {\"firstName\": \"John\", \"lastName\": \"Smith\", \"username\": \"CN999999\"}, \"determinationTs\": \"2022-12-19T20:40:03.208Z\", \"explanationCd\": \"4A\", \"explanationDesc\": \"Denied by Medical Services\", \"isDraft\": false, \"status\": \"DENIED\", \"typeCd\": \"\", \"typeDesc\": \"\", \"updatedBy\": {\"firstName\": \"\", \"lastName\": \"\", \"username\": \"\"}, \"updatedTs\": \"2022-12-19T20:40:13.585Z\", \"version\": 1, \"approvedUnitInfo\": {\"fromDate\": null, \"medicalNecessityCd\": \"\", \"medicalNecessityDesc\": \"\", \"toDate\": null, \"units\": 0, \"varianceCd\": \"\", \"varianceDesc\": \"\"}, \"deniedUnitInfo\": {\"fromDate\": null, \"medicalNecessityCd\": \"Not met\", \"medicalNecessityDesc\": \"Not met\", \"toDate\": null, \"units\": 1, \"varianceCd\": \"clinical_trial\", \"varianceDesc\": \"Clinical Trial\"}, \"pendingUnitInfo\": {\"fromDate\": null, \"medicalNecessityCd\": \"\", \"medicalNecessityDesc\": \"\", \"toDate\": null, \"units\": 0, \"varianceCd\": \"\", \"varianceDesc\": \"\"}}}")),
            inferDocumentSchema(
                BsonDocument.parse(
                    "{\"_id\": \"63a0cca0044eb7f573d238e1\", \"advisorReviewRequired\": false, \"createdBy\": {\"firstName\": \"John\", \"lastName\": \"Smith\", \"username\": \"CN999999\"}, \"createdTs\": \"2022-12-19T20:42:08.194Z\", \"isDraft\": false, \"status\": \"\", \"type\": \"determination\", \"updatedBy\": {\"firstName\": \"John\", \"lastName\": \"Smith\", \"username\": \"CN999999\"}, \"updatedTs\": \"2022-12-19T20:42:08.194Z\", \"version\": 0, \"voidInfo\": {\"reason\": {\"name\": \"\", \"code\": \"\", \"description\": \"\"}, \"otherReason\": \"\", \"details\": \"\", \"voidedById\": \"\", \"voidedByName\": \"\", \"ts\": null}, \"determination\": {\"_id\": \"63a0cca0044eb7f573d238e1\", \"appealId\": \"4N6M-WA3HEW\", \"claimsInstruction\": \"gdfgdfg\", \"completeInd\": true, \"createdBy\": {\"firstName\": \"\", \"lastName\": \"\", \"username\": \"\"}, \"createdTs\": \"2022-12-19T20:41:50.044Z\", \"determinedBy\": {\"firstName\": \"John\", \"lastName\": \"Smith\", \"username\": \"CN999999\"}, \"determinationTs\": \"2022-12-19T20:41:50.047Z\", \"explanationCd\": \"aR\", \"explanationDesc\": \"Avoidable Readmission\", \"isDraft\": false, \"status\": \"APPROVED\", \"typeCd\": \"\", \"typeDesc\": \"\", \"updatedBy\": {\"firstName\": \"\", \"lastName\": \"\", \"username\": \"\"}, \"updatedTs\": \"2022-12-19T20:42:07.511Z\", \"version\": 1, \"approvedUnitInfo\": {\"fromDate\": null, \"medicalNecessityCd\": \"Administrative denial\", \"medicalNecessityDesc\": \"Administrative denial\", \"toDate\": null, \"units\": 1, \"varianceCd\": \"awaiting_guardianship\", \"varianceDesc\": \"Awaiting Guardianship\"}, \"deniedUnitInfo\": {\"fromDate\": null, \"medicalNecessityCd\": \"\", \"medicalNecessityDesc\": \"\", \"toDate\": null, \"units\": 0, \"varianceCd\": \"\", \"varianceDesc\": \"\"}, \"pendingUnitInfo\": {\"fromDate\": null, \"medicalNecessityCd\": \"\", \"medicalNecessityDesc\": \"\", \"toDate\": null, \"units\": 0, \"varianceCd\": \"\", \"varianceDesc\": \"\"}}}")),
            inferDocumentSchema(
                BsonDocument.parse(
                    "{\"_id\": \"63a0d7bc044eb7f573d238e8\", \"reviewId\": \"1\", \"line\": 1, \"appealId\": \"4N6M-WA3HEW\", \"advisorReviewRequired\": false, \"completedTs\": \"2022-12-19T21:29:37.489Z\", \"createdBy\": {\"firstName\": \"\", \"lastName\": \"\", \"username\": \"\"}, \"criteriaSource\": \"Manual Review\", \"isDraft\": false, \"rationale\": \"asdasd\", \"reviewedById\": \"CN999999\", \"reviewedByName\": \"John Smith\", \"reviewerTypeCd\": \"CLINICIAN\", \"reviewerTypeDesc\": \"CLINICIAN\", \"reviewNotes\": \"asdasd\", \"reviewStartTs\": \"2022-12-19T21:29:32.145Z\", \"reviewSummary\": \"azsdasd\", \"status\": \"FORWARD FOR LEVEL 2 REVIEW\", \"type\": \"appeal\", \"updatedBy\": {\"firstName\": \"John\", \"lastName\": \"Smith\", \"username\": \"CN999999\"}, \"updatedTs\": \"2022-12-19T21:29:37.796Z\", \"version\": 3, \"voidInfo\": {\"reason\": {\"name\": \"\", \"code\": \"\", \"description\": \"\"}, \"otherReason\": \"\", \"details\": \"\", \"voidedById\": \"\", \"voidedByName\": \"\", \"ts\": null}}")),
            inferDocumentSchema(
                BsonDocument.parse(
                    "{\"_id\": \"63a0c7cc61152965f5f15b0c\", \"advisorReviewRequired\": false, \"createdBy\": {\"firstName\": \"John\", \"lastName\": \"Smith\", \"username\": \"CN999999\"}, \"createdTs\": \"2022-12-19T20:21:32.076Z\", \"isDraft\": false, \"status\": \"\", \"type\": \"determination\", \"updatedBy\": {\"firstName\": \"John\", \"lastName\": \"Smith\", \"username\": \"CN999999\"}, \"updatedTs\": \"2022-12-19T20:21:32.076Z\", \"version\": 0, \"voidInfo\": {\"reason\": {\"name\": \"\", \"code\": \"\", \"description\": \"\"}, \"otherReason\": \"\", \"details\": \"\", \"voidedById\": \"\", \"voidedByName\": \"\", \"ts\": null}, \"determination\": {\"_id\": \"63a0c7cc61152965f5f15b0c\", \"authorization_id\": \"63a0a143044eb7f573d238aa\", \"line\": 2, \"claimsInstruction\": \"sfsdfsdf\", \"completeInd\": true, \"createdBy\": {\"firstName\": \"John\", \"lastName\": \"Smith\", \"username\": \"CN999999\"}, \"createdTs\": \"2022-12-19T20:21:31.762Z\", \"determinedBy\": {\"firstName\": \"John\", \"lastName\": \"Smith\", \"username\": \"CN999999\"}, \"determinationTs\": \"2022-12-19T20:21:15.346Z\", \"explanationCd\": \"EX+W\", \"explanationDesc\": \"AUTH WITHDRAWN BY REQUESTOR\", \"isCurrent\": false, \"isDraft\": false, \"status\": \"DENIAL\", \"typeCd\": \"Appeal\", \"typeDesc\": \"Appeal Determination\", \"updatedBy\": {\"firstName\": \"John\", \"lastName\": \"Smith\", \"username\": \"CN999999\"}, \"updatedTs\": \"2022-12-19T20:21:31.762Z\", \"version\": 1, \"approvedUnitInfo\": {\"fromDate\": null, \"medicalNecessityCd\": \"\", \"medicalNecessityDesc\": \"\", \"toDate\": null, \"units\": 0, \"varianceCd\": \"\", \"varianceDesc\": \"\"}, \"deniedUnitInfo\": {\"fromDate\": \"2022-12-19T00:00:00Z\", \"medicalNecessityCd\": \"Administrative Denial\", \"medicalNecessityDesc\": \"Administrative Denial\", \"toDate\": \"2022-12-22T00:00:00Z\", \"units\": 3, \"varianceCd\": \"72HrReadmit\", \"varianceDesc\": \"72-Hour Readmission\"}, \"pendingUnitInfo\": {\"fromDate\": null, \"medicalNecessityCd\": \"\", \"medicalNecessityDesc\": \"\", \"toDate\": null, \"units\": 0, \"varianceCd\": \"\", \"varianceDesc\": \"\"}}}")),
            inferDocumentSchema(
                BsonDocument.parse(
                    "{\"_id\": \"63a0cc1d044eb7f573d238d9\", \"advisorReviewRequired\": false, \"createdBy\": {\"firstName\": \"John\", \"lastName\": \"Smith\", \"username\": \"CN999999\"}, \"createdTs\": \"2022-12-19T20:39:57.132Z\", \"isDraft\": false, \"status\": \"\", \"type\": \"determination\", \"updatedBy\": {\"firstName\": \"John\", \"lastName\": \"Smith\", \"username\": \"CN999999\"}, \"updatedTs\": \"2022-12-19T20:39:57.132Z\", \"version\": 0, \"voidInfo\": {\"reason\": {\"name\": \"\", \"code\": \"\", \"description\": \"\"}, \"otherReason\": \"\", \"details\": \"\", \"voidedById\": \"\", \"voidedByName\": \"\", \"ts\": null}, \"determination\": {\"_id\": \"63a0cc1d044eb7f573d238d9\", \"appealId\": \"WHPR-N7F7FM\", \"claimsInstruction\": \"sfsdfsdf\", \"completeInd\": true, \"createdBy\": {\"firstName\": \"\", \"lastName\": \"\", \"username\": \"\"}, \"createdTs\": \"2022-12-19T20:39:16.712Z\", \"determinedBy\": {\"firstName\": \"John\", \"lastName\": \"Smith\", \"username\": \"CN999999\"}, \"determinationTs\": \"2022-12-19T20:39:16.715Z\", \"explanationCd\": \"aM\", \"explanationDesc\": \"Admin Denial\", \"isDraft\": false, \"status\": \"PARTIAL APPROVAL\", \"typeCd\": \"\", \"typeDesc\": \"\", \"updatedBy\": {\"firstName\": \"\", \"lastName\": \"\", \"username\": \"\"}, \"updatedTs\": \"2022-12-19T20:39:56.756Z\", \"version\": 1, \"approvedUnitInfo\": {\"fromDate\": null, \"medicalNecessityCd\": \"Met at lower level\", \"medicalNecessityDesc\": \"Met at lower level\", \"toDate\": null, \"units\": 1, \"varianceCd\": \"awaiting_consult\", \"varianceDesc\": \"Awaiting Consult\"}, \"deniedUnitInfo\": {\"fromDate\": null, \"medicalNecessityCd\": \"Met as requested\", \"medicalNecessityDesc\": \"Met as requested\", \"toDate\": null, \"units\": 2, \"varianceCd\": \"awaiting_guardianship\", \"varianceDesc\": \"Awaiting Guardianship\"}, \"pendingUnitInfo\": {\"fromDate\": null, \"medicalNecessityCd\": \"\", \"medicalNecessityDesc\": \"\", \"toDate\": null, \"units\": 0, \"varianceCd\": \"\", \"varianceDesc\": \"\"}}}")));

    for (int i = 0; i < schemas.size(); i++) {
      for (int j = 0; j < schemas.size(); j++) {
        if (i == j) {
          continue;
        }
        System.out.println("Comparing " + i + " to " + j);
        diffSchemas("", schemas.get(i), schemas.get(j));
        System.out.println();
        System.out.println("Comparing " + j + " to " + i);
        diffSchemas("", schemas.get(j), schemas.get(i));
        System.out.println();
      }
    }
  }

  private static void diffSchemas(String indent, Schema firstSchema, Schema secondSchema) {
    if (firstSchema.type() != Schema.Type.STRUCT) {
      System.out.println(indent + "first schema is not a struct");
      return;
    }
    if (secondSchema.type() != Schema.Type.STRUCT) {
      System.out.println(indent + "second schema is not a struct");
      return;
    }
    for (int i = 0; i < firstSchema.fields().size(); i++) {
      Field firstField = firstSchema.fields().get(i);
      Field secondField = secondSchema.field(firstField.name());
      if (secondField == null) {
        System.out.println(indent + "No matching field in second schema for " + firstField);
      } else if (!firstField.schema().equals(secondField.schema())) {
        System.out.println(indent + firstField);
        System.out.println(indent + secondField);
        diffSchemas(indent + "  ", firstField.schema(), secondField.schema());
      }
    }
    if (secondSchema.fields().size() > firstSchema.fields().size()) {
      System.out.printf(
          indent + "Second schema has %d more fields than first%n",
          secondSchema.fields().size() - firstSchema.fields().size());
    }
  }
}
