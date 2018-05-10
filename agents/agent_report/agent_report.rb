class AgentReport < AbstractReport

  register_report

  def headers
    ['uri', 'heading', 'type', 'source', 'authority_id', 'linked_records']
  end

  def query
    people_links = db[:agent_person]
      .left_outer_join(:linked_agents_rlshp, :agent_person_id => :agent_person__id)
      .select(Sequel.as(:agent_person__id, :id))
      .select_more{Sequel.as(count(:linked_agents_rlshp__id), :links)}
      .group(:agent_person__id)

    people = db[:agent_person]
      .left_outer_join(:name_person, :agent_person_id => :agent_person__id)
      .left_outer_join(people_links, {:id => :agent_person__id}, :table_alias => :people_links)
      .left_outer_join(:enumeration_value, {:id => :name_person__source_id}, :table_alias => :source)
      .left_outer_join(:name_authority_id, :name_person_id => :name_person__id)
      .filter(:name_person__is_display_name => 1)
      .filter(db[:user]
        .filter(:agent_record_id => :name_person__agent_person_id)
        .select(:agent_record_id) => nil)
      .select(Sequel.as(Sequel.lit("concat('/agents/people/',agent_person.id)"), :uri),
              Sequel.as(:name_person__sort_name, :heading),
              Sequel.as('person', :type),
              Sequel.as(:source__value, :source),
              Sequel.as(:name_authority_id__authority_id, :authority_id),
              Sequel.as(:people_links__links, :linked_records))

    corporate_links = db[:agent_corporate_entity]
      .left_outer_join(:linked_agents_rlshp, :agent_corporate_entity_id => :agent_corporate_entity__id)
      .select(Sequel.as(:agent_corporate_entity__id, :id))
      .select_more{Sequel.as(count(:linked_agents_rlshp__id), :links)}
      .group(:agent_corporate_entity__id)

    corporate = db[:agent_corporate_entity]
      .left_outer_join(:name_corporate_entity, :agent_corporate_entity_id => :agent_corporate_entity__id)
      .left_outer_join(corporate_links, {:id => :agent_corporate_entity__id}, :table_alias => :corporate_links)
      .left_outer_join(:enumeration_value, {:id => :name_corporate_entity__source_id}, :table_alias => :source)
      .left_outer_join(:name_authority_id, :name_corporate_entity_id => :name_corporate_entity__id)
      .filter(:name_corporate_entity__is_display_name => 1)
      .select(Sequel.as(Sequel.lit("concat('/agents/corporate_entities/',agent_corporate_entity.id)"), :uri),
              Sequel.as(:name_corporate_entity__sort_name, :heading),
              Sequel.as('corporate_entity', :type),
              Sequel.as(:source__value, :source),
              Sequel.as(:name_authority_id__authority_id, :authority_id),
              Sequel.as(:corporate_links__links, :linked_records))

    families_links = db[:agent_family]
      .left_outer_join(:linked_agents_rlshp, :agent_family_id => :agent_family__id)
      .select(Sequel.as(:agent_family__id, :id))
      .select_more{Sequel.as(count(:linked_agents_rlshp__id), :links)}
      .group(:agent_family__id)

    families = db[:agent_family]
      .left_outer_join(:name_family, :agent_family_id => :agent_family__id)
      .left_outer_join(families_links, {:id => :agent_family__id}, :table_alias => :families_links)
      .left_outer_join(:enumeration_value, {:id => :name_family__source_id}, :table_alias => :source)
      .left_outer_join(:name_authority_id, :name_family_id => :name_family__id)
      .filter(:name_family__is_display_name => 1)
      .select(Sequel.as(Sequel.lit("concat('/agents/families/',agent_family.id)"), :uri),
              Sequel.as(:name_family__sort_name, :heading),
              Sequel.as('family', :type),
              Sequel.as(:source__value, :source),
              Sequel.as(:name_authority_id__authority_id, :authority_id),
              Sequel.as(:families_links__links, :linked_records))

    people.union(corporate).union(families).order(Sequel.asc(:heading))

  end
end
