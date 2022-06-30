import json
import typing
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

POS_PUNCTUATION = ['.', '"', ',', '(', ')', ':', '$', "''"]


class Pipeline:

    @staticmethod
    def get_ner_list(sentence, mentions, ner_labels):
        sentence_length = len(sentence.replace("\n", " ").strip().split(" "))
        ner_list = ["0"] * sentence_length
        for mention in mentions:
            if mention in sentence:
                for i in range(sentence_length):
                    if sentence.replace("\n", " ").strip().split(" ")[i] == mention.split(" ")[0]:
                        if ner_list[i-1] == "I-" + LABEL_MAP[ner_labels[mentions.index(mention)]]:
                            for w in range(len(mention.split(" "))):
                                ner_list[i + w] = "B-" + LABEL_MAP[ner_labels[mentions.index(mention)]]
                        else:
                            for w in range(len(mention.split(" "))):
                                ner_list[i + w] = "I-" + LABEL_MAP[ner_labels[mentions.index(mention)]]
        return ner_list

    @staticmethod
    def get_pos_list(sentence, mentions, pos_labels):
        sentence_length = len(sentence.replace("\n", " ").strip().split(" "))
        pos_list = [word if word in POS_PUNCTUATION else "NN" for word in sentence.replace("\n", " ").strip().split(" ")]
        for mention in mentions:
            if mention in sentence:
                for i in range(sentence_length):
                    if sentence.replace("\n", " ").strip().split(" ")[i] == mention.split(" ")[0]:
                        for w in range(len(mention.split(" "))):
                            pos_list[i + w] = pos_labels[mentions.index(mention)]
        return pos_list

    @staticmethod
    def extract_node_properties(data: typing.List[typing.Dict]) -> typing.List[typing.Dict]:
        nodes = []
        for datapoint in data:
            intermediary_dict = {}
            for key, node in datapoint.items():
                intermediary_dict[key] = node._properties
            nodes.append(intermediary_dict)
        return nodes

    @staticmethod
    def convert_labels(label: str):
        return LABEL_MAP[label]

    def run_ongdb_query(self):
        sentence_gql = 'MATCH (sentence:Sentence) RETURN sentence'
        sentiment_gql = 'MATCH (sentiment:SentenceSentiment)<-[hs:HAS_SENTIMENT]-(sentence:Sentence) RETURN sentence, sentiment'
        mention_gql = 'MATCH (sentence:Sentence)-[hm:HAS_MENTION]->(mention:Mention) WHERE mention.language = "en" RETURN sentence, mention'
        keyphrase_gql = 'MATCH (sentence:Sentence)-[hm:HAS_KEYPHRASE]->(keyphrase:Keyphrase) RETURN sentence, keyphrase'
        re_gql = 'MATCH (mention1:Mention)-[relationship]->(mention2:Mention) return mention1, relationship, mention2'
        # with neo4j.GraphDatabase.driver('bolt://localhost:7687', auth=(
        with neo4j.GraphDatabase.driver('bolt://ongdb:7687', auth=(
                'ongdb',
                'admin')) as local_driver, local_driver.session() as local_session:
            sentence_results: neo4j.BoltStatementResult = local_session.run(sentence_gql)
            sentiment_results: neo4j.BoltStatementResult = local_session.run(sentiment_gql)
            mention_results: neo4j.BoltStatementResult = local_session.run(mention_gql)
            keyphrase_results: neo4j.BoltStatementResult = local_session.run(keyphrase_gql)
            relation_results: neo4j.BoltStatementResult = local_session.run(re_gql)

            sentence_nodes = sentence_results.data('sentence')
            sentiment_nodes = sentiment_results.data('sentence', 'sentiment')
            mention_nodes = mention_results.data('sentence', 'mention')
            keyphrase_nodes = keyphrase_results.data('sentence', 'keyphrase')
            relation_nodes = relation_results.data('mention1', 'relationship', 'mention2')
        sentence_nodes = self.extract_node_properties(sentence_nodes)
        sentiment_nodes = self.extract_node_properties(sentiment_nodes)
        mention_nodes = self.extract_node_properties(mention_nodes)
        keyphrase_nodes = self.extract_node_properties(keyphrase_nodes)
        relation_nodes = self.extract_node_properties(relation_nodes)
        for sentence in sentence_nodes:
            for s in sentiment_nodes:
                if s["sentence"]["sentence"] == sentence["sentence"]["sentence"]:
                    sentence["sentiment"] = s["sentiment"]
            mentions = []
            for m in mention_nodes:
                if m["sentence"]["sentence"] == sentence["sentence"]["sentence"]:
                    mentions.append(m["mention"])
            sentence["mentions"] = mentions
            keyphrases = []
            for k in keyphrase_nodes:
                if k["sentence"]["sentence"] == sentence["sentence"]["sentence"]:
                    keyphrases.append(k["keyphrase"])
            sentence["keyphrases"] = keyphrases
            relations = []
            for r in relation_nodes:
                if r["mention1"] in sentence["mentions"] and r["mention2"] in sentence["mentions"]:
                    relations.append({"sub": r["mention1"], "obj": r["mention2"], "relation": r["relationship"]})
            sentence["relations"] = relations
        return [sentence_nodes]

    def save_dataset(self, nodes, filename: str):
        sdk = GraphGridSdk()

        def transform_data(nodes):
            for datapoint in nodes:
                sentence: str = datapoint.get('sentence').get('sentence')
                sentiment: int = datapoint.get('sentiment').get('sentiment')
                mentions: [str] = [mention.get('value') for mention in datapoint.get('mentions')]
                # mention_words: [str] = [word for mention in mentions for word in mention]  # splits list of mentions into list of words
                # mention_words = [mention.split(' ') for mention in mentions]
                # words: [str] = [word for mention in mentions for word in mention]
                ner_labels: [str] = [mention.get('ne')[0] for mention in datapoint.get('mentions')]
                pos_labels: [str] = [mention.get('pos')[0] for mention in datapoint.get('mentions')]
                keyphrases: [str] = [keyphrase.get('keyphraseId') for keyphrase in datapoint.get('keyphrases')]
                relations: [dict] = [
                    {"sub": {"entity": relation["sub"]['value'], "type": self.convert_labels(relation["sub"]['ne'][0])},
                     "obj": {"entity": relation["obj"]['value'], "type": self.convert_labels(relation["obj"]['ne'][0])},
                     "relation": type(relation)} for relation in datapoint.get('relations')]  # Relation type doesn't work

                annotated_sentence_dict: dict = {
                    "sentence": sentence,
                    "named_entity": self.get_ner_list(sentence, mentions, ner_labels),
                    "pos": self.get_pos_list(sentence, mentions, pos_labels),
                    "sentiment": {"binary": sentiment, "categorical": sentiment},  # need to include a check for binary or categorical!!!
                    "keyphrases": keyphrases,
                    "relations": relations,
                }
                annotated_sentence = json.dumps(annotated_sentence_dict).encode()
                yield annotated_sentence

        yield_function = transform_data(nodes)
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
