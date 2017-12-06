class AgentReport < AbstractReport

  register_report

  def headers
    ['name', 'type', 'links', 'source', 'url', 'verified']
  end

  def processor
    {
      'verified' => proc {|record| verify_source(record[:source], record[:url])}
    }
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
      .select(Sequel.as(:name_person__sort_name, :name),
              Sequel.as('person', :type),
              Sequel.as(:people_links__links, :links),
              Sequel.as(:source__value, :source),
              Sequel.as(:name_authority_id__authority_id, :url))

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
      .select(Sequel.as(:name_corporate_entity__sort_name, :name),
              Sequel.as('corporate_entity', :type),
              Sequel.as(:corporate_links__links, :links),
              Sequel.as(:source__value, :source),
              Sequel.as(:name_authority_id__authority_id, :url))

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
      .select(Sequel.as(:name_family__sort_name, :name),
              Sequel.as('family', :type),
              Sequel.as(:families_links__links, :links),
              Sequel.as(:source__value, :source),
              Sequel.as(:name_authority_id__authority_id, :url))

    people
      .union(corporate)
      .union(families)
      .order(Sequel.asc(:name))
  end

  private

  def verify_source(source, url)
    locals = ['ingest', 'local', 'prov']
    # TODO: I know the prefix for NAD is wrong, I'm just pretty sure ours are all LCNAF entries
    uri_prefix = {
      'nad' => "http://id.loc.gov/authorities/names",
      'naf' => "http://id.loc.gov/authorities/names",
      'ulan' => "http://vocab.getty.edu/ulan",
      'lcsh' => "http://id.loc.gov/authorities/subjects",
      'viaf' => "http://viaf.org/viaf"
    }

    if ASUtils.blank?(url)
      locals.include?(source) ? url : "ERROR: Authority ID must be present if a non-local source is declared"
    else
      if locals.include?(source)
        "Success"
      else
        prefix = uri_prefix[source]
        url.start_with?(prefix) ? "Success" : "ERROR: Authority ID for #{source} must begin with #{prefix}"
      end
    end
  end
end
