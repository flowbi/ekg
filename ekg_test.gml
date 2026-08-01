graph [
  directed 1
  node [
    id 0
    label "Customer_42"
    entity_label "Customer"
    name "Jane Doe"
    email__privacy_class "PII"
  ]
  node [
    id 1
    label "Link_1"
    entity_label "Order"
    amount 100
  ]
  edge [
    source 1
    target 0
    entity_label "REFERENCES"
    ekg_id "edge1"
    since 2024
  ]
]
