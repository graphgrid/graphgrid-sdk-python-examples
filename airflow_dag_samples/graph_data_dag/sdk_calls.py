import json
import os
import typing
import unicodedata

import fire
import neo4j
from graphgrid_sdk.ggcore.sdk_messages import SaveDatasetResponse
from graphgrid_sdk.ggsdk.sdk import GraphGridSdk

LABEL_MAP = {
    "ORGANIZATION": "ORG",
    "LOCATION": "LOC",
    "PERSON": "PER",
    "MISCELLANEOUS": "MISC"
}


class Pipeline:

    def __init__(self):
        pass

    @staticmethod
    def add_whitespace_to_punctuation(text):
        """Splits punctuation on a piece of text."""

        def is_punctuation(char):
            cp = ord(char)
            # We treat all non-letter/number ASCII as punctuation.
            # Characters such as "^", "$", and "`" are not in the Unicode
            # Punctuation class but we treat them as punctuation anyways, for
            # consistency.
            if ((cp >= 33 and cp <= 47) or (cp >= 58 and cp <= 64) or
                    (cp >= 91 and cp <= 96) or (cp >= 123 and cp <= 126)):
                return True
            cat = unicodedata.category(char)
            if cat.startswith("P"):
                return True
            return False

        chars = list(text)
        i = 0
        output = ""
        while i < len(chars):
            char = chars[i]
            if is_punctuation(char):
                output = output + " " + char
            else:
                output = output + char
            i += 1
        return output

    @staticmethod
    def get_ner_list(sentence, mentions):
        split_sentence = sentence.strip().split(" ")
        sentence_length = len(split_sentence)
        ner_list = ["O"] * sentence_length
        # Find each mention in the sentence, put the mention labels at those indexes in ner_list
        for mention in mentions:
            ner_label = LABEL_MAP[mention.get("ne")[0]]
            split_mention = mention.get("value").split(" ")
            mention_length = len(split_mention)
            # Check each word in the sentence to see if it's the beginning of the mention
            i = 0
            while i < sentence_length:
                for j in range(mention_length):
                    # If the sentence has only part of the mention, ignore it.
                    try:
                        if split_sentence[i + j] != split_mention[j]:
                            i = i + 1
                            break
                    except IndexError:
                        i = i + 1
                        break
                    if j == mention_length - 1:
                        # If the previous word has the same label, differentiate the current mention with "B-"
                        if ner_list[i - 1] != 'I-' + ner_label or i == 0:
                            for k in range(mention_length):
                                ner_list[i + k] = 'I-' + ner_label
                            i = i + mention_length
                        else:
                            for k in range(mention_length):
                                ner_list[i + k] = 'B-' + ner_label
                            i = i + mention_length
        return ner_list

    @staticmethod
    def extract_node_properties(data: typing.List[typing.Dict]) -> typing.List[
        typing.Dict]:
        nodes = []
        for datapoint in data:
            intermediary_dict = {}
            for key, node in datapoint.items():
                intermediary_dict[key] = node._properties
                if hasattr(node, "type"):
                    intermediary_dict["type"] = node.type
            nodes.append(intermediary_dict)
        return nodes

    @staticmethod
    def convert_labels(label: str):
        return LABEL_MAP[label]

    def run_ongdb_query(self):

        filename = 'training_file.jsonl'

        article_gql = 'MATCH (article:Article)-[hat:HAS_ANNOTATED_TEXT]->(at:AnnotatedText) WHERE article.language="en" RETURN at'
        sentence_gql = 'MATCH (at:AnnotatedText)-[cs:CONTAINS_SENTENCE]->(sentence:Sentence) WHERE at.grn=$at_grn RETURN sentence'
        translation_language_gql = 'MATCH (origArticle:Article)-[ht:HAS_TRANSLATION]->(enArticle:Article)-[hat:HAS_ANNOTATED_TEXT]->(at:AnnotatedText) WHERE at.grn = $at_grn RETURN origArticle'
        translation_gql = 'MATCH (origSentence:Sentence)-[ts:TRANSLATED_SENTENCE]->(englishSentence:Sentence) WHERE englishSentence.grn=$s_grn RETURN origSentence'
        mention_gql = 'MATCH (sentence:Sentence)-[hm:HAS_MENTION]->(mention:Mention) WHERE sentence.grn=$s_grn RETURN mention'
        keyphrase_gql = 'MATCH (sentence:Sentence)-[hk:HAS_KEYPHRASE]->(keyphrase:Keyphrase) WHERE sentence.grn=$s_grn RETURN keyphrase'
        re_gql = 'MATCH (m1:Mention)-[relationship]->(m2:Mention) WHERE m1.grn=$m1_grn AND m2.grn=$m2_grn AND relationship.sentenceGrn=$s_grn RETURN relationship'

        with neo4j.GraphDatabase.driver('bolt://ongdb:7687', auth=(
                'ongdb',
                'admin')) as local_driver, local_driver.session() as local_session:
            annotated_text_results: neo4j.BoltStatementResult = local_session.run(
                article_gql)
            annotated_text_nodes = annotated_text_results.data('at')
            extracted_at_nodes = self.extract_node_properties(
                annotated_text_nodes)
            for at in extracted_at_nodes:
                at_grn = at.get("at").get("grn")
                translated_text = False
                translation_language_results: neo4j.BoltStatementResult = local_session.run(
                    translation_language_gql, {"at_grn": at_grn})
                translation_language_nodes = translation_language_results.data(
                    'origArticle')
                extracted_translation_language_nodes = self.extract_node_properties(
                    translation_language_nodes)
                if len(extracted_translation_language_nodes) > 0:
                    translated_text = True
                    orig_language = extracted_translation_language_nodes[0].get(
                        "origArticle").get("language")
                sentence_results: neo4j.BoltStatementResult = local_session.run(
                    sentence_gql, {"at_grn": at_grn})
                sentence_nodes = sentence_results.data('sentence')
                extracted_sentence_nodes = self.extract_node_properties(
                    sentence_nodes)
                for sentence in extracted_sentence_nodes:
                    sentence_json = {}
                    s_grn = sentence.get("sentence").get("grn")
                    translation_results: neo4j.BoltStatementResult = local_session.run(
                        translation_gql, {"s_grn": s_grn})
                    translation_nodes = translation_results.data('origSentence')
                    extracted_translation_nodes = self.extract_node_properties(
                        translation_nodes)
                    keyphrase_results: neo4j.BoltStatementResult = local_session.run(
                        keyphrase_gql, {"s_grn": s_grn})
                    keyphrase_nodes = keyphrase_results.data('keyphrase')
                    extracted_keyphrase_nodes = self.extract_node_properties(
                        keyphrase_nodes)
                    mention_results: neo4j.BoltStatementResult = local_session.run(
                        mention_gql, {"s_grn": s_grn})
                    mention_nodes = mention_results.data('mention')
                    extracted_mention_nodes = self.extract_node_properties(
                        mention_nodes)
                    relations = []
                    for i in range(len(extracted_mention_nodes)):
                        m1 = extracted_mention_nodes[i].get("mention")
                        m1_grn = m1.get("grn")
                        for j in range(len(extracted_mention_nodes)):
                            m2 = extracted_mention_nodes[j].get("mention")
                            m2_grn = m2.get("grn")
                            relation_results: neo4j.BoltStatementResult = local_session.run(
                                re_gql, {"m1_grn": m1_grn, "m2_grn": m2_grn,
                                         "s_grn": s_grn})
                            re_relationships = relation_results.data(
                                "relationship")
                            extracted_re_relationships = self.extract_node_properties(
                                re_relationships)
                            for relationship in extracted_re_relationships:
                                relations.append({"obj": {
                                    "entity": m2.get("value"),
                                    "type": self.convert_labels(
                                        m2.get("ne")[0])}, "sub": {
                                    "entity": m1.get("value"),
                                    "type": self.convert_labels(
                                        m1.get("ne")[0])},
                                                  "relation": relationship.get(
                                                      "type")})
                    sentence_text = sentence.get("sentence").get("sentence")
                    sentence_text = self.add_whitespace_to_punctuation(
                        sentence_text)
                    mentions = [mention.get("mention") for mention in
                                extracted_mention_nodes]
                    sentence_json["sentence"] = sentence_text
                    if translated_text and len(extracted_translation_nodes) > 0:
                        sentence_json["translations"] = {}
                        sentence_json["translations"][orig_language] = \
                        extracted_translation_nodes[0].get("origSentence").get(
                            "sentence")
                    sentence_json["keyphrases"] = [
                        keyphrase.get("keyphrase").get("keyphraseId") for
                        keyphrase in extracted_keyphrase_nodes]
                    sentence_json["named_entity"] = self.get_ner_list(
                        sentence_text, mentions)
                    sentence_json["relations"] = relations

                    with open(os.path.join('/volumes', filename), 'a+',
                              encoding='utf-8') as f:
                        f.write(json.dumps(sentence_json))
                        f.write('\n')

        return filename

    def save_dataset(self, filename: str):
        sdk = GraphGridSdk()

        def read_by_line(dataset_filepath):
            infile = open(
                dataset_filepath,
                'r', encoding='utf8')
            for line in infile:
                yield line.encode()

        filepath = os.path.join('/volumes', filename)
        yield_function = read_by_line(filepath)
        dataset_response: SaveDatasetResponse = sdk.save_dataset(yield_function,
                                                                 filename)
        if dataset_response.status_code != 200:
            raise Exception("Failed to save dataset: ",
                            dataset_response.exception)

        return dataset_response.datasetId

    def train_and_promote(self, models_to_train: list,
                          datasetId: str,
                          no_cache: typing.Optional[bool],
                          gpu: typing.Optional[bool], autopromote: bool):
        sdk = GraphGridSdk()

        def success_handler(args):
            return

        def failed_handler(args):
            return

        sdk.nmt_train_pipeline(models_to_train, datasetId, no_cache, gpu,
                               autopromote, success_handler, failed_handler)


def main():
    fire.Fire(Pipeline)


if __name__ == '__main__':
    main()
