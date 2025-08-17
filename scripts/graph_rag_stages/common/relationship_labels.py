# scripts/graph_rag_stages/common/relationship_labels.py
import re

# Canonical mapping: keys are normalized to alphanumeric lower-case (punctuation removed).
_RELABEL = {
    # topical
    "hastopic": "addressesTopic",
    "has_topic": "addressesTopic",     # tolerated raw
    "addressestopic": "addressesTopic",
    "addresses": "addressesTopic",

    # taxonomy/ner common
    "hasdocument": "hasDocument",
    "isabout": "isAbout",
    "pertainsto": "isAbout",
    "isrecordof": "isRecordOf",

    # sectioning / agenda
    "insection": "belongsToSection",   # AgendaItem → Section
    "belongstosection": "belongsToSection",
    "belongstoagenda": "isPartOf",
    "containsitem": "containsItem",    # Section → AgendaItem
    "hasagendaitem": "containsItem",
    "hassection": "hasSection",

    # events / docs
    "discussedin": "discussedIn",
    "discusses": "discusses",
    "recordedin": "recordedIn",
    "hastranscript": "hasTranscript",
    "occursat": "occursAt",
    "authoredby": "authoredBy",

    # policy/vote
    "votedon": "votedOn",
    "adoptedat": "adoptedAt",
    "decidedat": "adoptedAt",
    "enactspolicy": "enactsPolicy",
    "sponsorof": "sponsors",
    "sponsors": "sponsors",

    # broader/narrower/related
    "broader": "broaderThan",
    "broaderthan": "broaderThan",
    "narrower": "narrowerThan",
    "narrowerthan": "narrowerThan",
    "related": "relatedTo",
    "relatedto": "relatedTo",

    # passthroughs
    "ispartof": "isPartOf",
    "ismemberof": "isMemberOf",
    "performsaction": "performsAction",
    # requested additions
    "locatedat": "isLocatedAt",
    "presentedat": "presents",
    "cites": "references",
    "contains": "containsItem",
}

def _key(s: str) -> str:
    return re.sub(r"[^a-z]", "", (s or "").lower())

def normalize_rel_label(label: str) -> str:
    """
    Canonicalize a relationship label across the whole pipeline.
    Strips punctuation/spaces/case, maps to the ontology-preferred verb.
    Unrecognized labels pass through unchanged.
    """
    k = _key(label)
    return _RELABEL.get(k, label)
