class ResourceAuditReport < AbstractReport

  register_report

  def headers
    ['title', 'call_number', 'dates', 'extent', 'creators', 'subjects', 'abstract', 'scopecontent', 'bioghist']
  end

  def processor
  {
    'call_number' => proc {|record| parse_identifier(record[:identifier])},
    'extent' => proc {|record| [record[:extentNumber], record[:extentType]].compact.join(' ')},
    'subjects' => proc {|record| [record[:subjectAgents], record[:subjectTerms]].compact.join('; ')},
    'abstract' => proc {|record| parse_note_singlepart(record[:noteAbstract])},
    'scopecontent' => proc {|record| parse_note_multipart(record[:noteScopeContent])},
    'bioghist' => proc {|record| parse_note_multipart(record[:noteBioghist])}
  }
  end

  def query
    repo_id = @params[:repo_id]
    
    type_abstract = '%"type":"abstract"%'
    type_scopecontent = '%"type":"scopecontent"%'
    type_bioghist = '%"type":"bioghist"%'

    # gather all dates into a single table
    dates = db[:resource]
      .left_outer_join(:date, :resource_id => :resource__id)
      .left_outer_join(:enumeration_value, {:id => :date__label_id}, :table_alias => :date_label)
      .left_outer_join(:enumeration_value, {:id => :date__date_type_id}, :table_alias => :date_date_type)
      .select(Sequel.as(:resource__id, :id),
              Sequel.as(:date__expression, :expression))
      .where(:date_label__value => 'creation')

    # filter inclusive and bulk dates based on the value of date_date_type.value
    inclusive_date = dates.where(:date_date_type__value => 'inclusive')

    # gather all linked agents into a single table, and filter based on their role
    linked_agents = get_linked_agents

    creators = linked_agents
      .select(Sequel.as(:id, :id),
              Sequel.as(Sequel.lit("group_concat(sort_name separator '\; ')"), :creators))
      .where(:role => 'creator')
      .group(:id)

    subject_agents = linked_agents
      .select(Sequel.as(:id, :id),
              Sequel.as(Sequel.lit("group_concat(sort_name separator '\; ')"), :subjects))
      .where(:role => 'subject')
      .group(:id)

    # gather all subject terms into a single table (we'll use this to concat subject_agents and subject_terms)
    subject_terms = db[:subject_rlshp]
      .left_outer_join(:subject, :id => :subject_rlshp__subject_id)
      .select(Sequel.as(:subject_rlshp__resource_id, :id),
              Sequel.as(Sequel.lit("group_concat(subject.title separator '\; ')"), :subjects))
      .group(:id)

    notes = db[:resource]
      .left_outer_join(:note, :resource_id => :resource__id)
      .select(Sequel.as(:resource__id, :id),
              Sequel.as(:note__notes, :notes))

    abstracts = notes.where(Sequel.like(:notes, type_abstract))
    scopecontents = notes.where(Sequel.like(:notes, type_scopecontent))
    bioghists = notes.where(Sequel.like(:notes, type_bioghist))

    # ...and now we run the main query
    db[:resource]
      .left_outer_join(inclusive_date, {:id => :resource__id}, :table_alias => :inclusive_date)
      .left_outer_join(:extent, :resource_id => :resource__id)
      .left_outer_join(:enumeration_value, {:id => :extent__extent_type_id}, :table_alias => :extent_type)
      .left_outer_join(creators, {:id => :resource__id}, :table_alias => :creator)
      .left_outer_join(subject_agents, {:id => :resource__id}, :table_alias => :subject_agent)
      .left_outer_join(subject_terms, {:id => :resource__id}, :table_alias => :subject_term)
      .left_outer_join(abstracts, {:id => :resource__id}, :table_alias => :note_abstract)
      .left_outer_join(scopecontents, {:id => :resource__id}, :table_alias => :note_scopecontent)
      .left_outer_join(bioghists, {:id => :resource__id}, :table_alias => :note_bioghist)
      .select(Sequel.as(:resource__id, :id),
              Sequel.as(:resource__title, :title),
              Sequel.as(:resource__identifier, :identifier),
              Sequel.as(:inclusive_date__expression, :dates),
              Sequel.as(:extent__number, :extentNumber),
              Sequel.as(:extent_type__value, :extentType),
              Sequel.as(:creator__creators, :creators),
              Sequel.as(:subject_agent__subjects, :subjectAgents),
              Sequel.as(:subject_term__subjects, :subjectTerms),
              Sequel.as(:note_abstract__notes, :noteAbstract),
              Sequel.as(:note_scopecontent__notes, :noteScopeContent),
              Sequel.as(:note_bioghist__notes, :noteBioghist))
      .filter(:resource__repo_id => repo_id)
      .order(Sequel.asc(:identifier))

  end

  private

  def get_linked_agents
    people = db[:linked_agents_rlshp]
      .left_outer_join(:agent_person, :id => :linked_agents_rlshp__agent_person_id)
      .left_outer_join(:name_person, :agent_person_id => :agent_person__id)
      .left_outer_join(:enumeration_value, {:id => :linked_agents_rlshp__role_id}, :table_alias => :linked_agent_role)
      .select(Sequel.as(:linked_agents_rlshp__resource_id, :id),
              Sequel.as(:name_person__sort_name, :sort_name),
              Sequel.as(:linked_agent_role__value, :role))
      .where(:name_person__is_display_name => true)

    corporate_entities = db[:linked_agents_rlshp]
      .left_outer_join(:agent_corporate_entity, :id => :linked_agents_rlshp__agent_corporate_entity_id)
      .left_outer_join(:name_corporate_entity, :agent_corporate_entity_id => :agent_corporate_entity__id)
      .left_outer_join(:enumeration_value, {:id => :linked_agents_rlshp__role_id}, :table_alias => :linked_agent_role)
      .select(Sequel.as(:linked_agents_rlshp__resource_id, :id),
              Sequel.as(:name_corporate_entity__sort_name, :sort_name),
              Sequel.as(:linked_agent_role__value, :role))
      .where(:name_corporate_entity__is_display_name => true)

    families = db[:linked_agents_rlshp]
      .left_outer_join(:agent_family, :id => :linked_agents_rlshp__agent_family_id)
      .left_outer_join(:name_family, :agent_family_id => :agent_family__id)
      .left_outer_join(:enumeration_value, {:id => :linked_agents_rlshp__role_id}, :table_alias => :linked_agent_role)
      .select(Sequel.as(:linked_agents_rlshp__resource_id, :id),
              Sequel.as(:name_family__sort_name, :sort_name),
              Sequel.as(:linked_agent_role__value, :role))
      .where(:name_family__is_display_name => true)

    dataset = people
      .union(corporate_entities)
      .union(families)

    dataset
  end

  def parse_identifier(s)
    if ASUtils.blank?(s)
      s
    else
      id = ASUtils.json_parse(s).compact[0]
    end
  end

  def parse_note_singlepart(s)
    if ASUtils.blank?(s)
      s
    else
      json = ASUtils.json_parse(s)
      content = json['content'].compact.join(' ').gsub(/<(\/)?p>/,'')
      content
    end
  end

  def parse_note_multipart(s)
    if ASUtils.blank?(s)
      s
    else
      json = ASUtils.json_parse(s)
      notes = []
      json['subnotes'].each {|subnote| notes.push(subnote['content'])}
      content = notes.compact.join(' ').gsub(/<(\/)?p>/,'')
      content
    end
  end

end
