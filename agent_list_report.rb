class AgentListReport < AbstractReport

  register_report

  def fix_row(row)
    ReportUtils.get_enum_values(row, [:name_source])
  end

  def query_string
    "(select
      concat('/agents/people/', agent_person.id) as uri,
      name_person.sort_name as heading,
      'Person' as name_type,
      name_person.source_id as name_source,
      name_authority_id.authority_id as authority_id,
      links.count as linked_records
    from agent_person
      left outer join name_person
        on name_person.agent_person_id = agent_person.id
      left outer join name_authority_id
        on name_authority_id.name_person_id = name_person.id
      left outer join (
        select
          agent_person.id as id,
          count(linked_agents_rlshp.id) as count
        from agent_person
          left outer join linked_agents_rlshp
            on linked_agents_rlshp.agent_person_id = agent_person.id
        group by agent_person.id
      ) as links
        on links.id = agent_person.id
      left outer join user
        on user.agent_record_id = name_person.agent_person_id
    where name_person.is_display_name
      and not name_person.source_id is null
      and user.id is null)

    union

    (select
      concat('/agents/families/',agent_family.id) as uri,
      name_family.sort_name as heading,
      'Family' as name_type,
      name_family.source_id as name_source,
      name_authority_id.authority_id as authority_id,
      links.count as linked_records
      from agent_family
        left outer join name_family
          on name_family.agent_family_id = agent_family.id
        left outer join name_authority_id
          on name_authority_id.name_family_id = name_family.id
        left outer join (
          select
            agent_family.id as id,
            count(linked_agents_rlshp.id) as count
          from agent_family
            left outer join linked_agents_rlshp
              on linked_agents_rlshp.agent_family_id = agent_family.id
          group by agent_family.id
        ) as links
          on links.id = agent_family.id
        left outer join user
          on user.agent_record_id = name_family.agent_family_id
    where name_family.is_display_name
      and not name_family.source_id is null
      and user.id is null)

    union

    (select
      concat('/agents/corporate_entities/',agent_corporate_entity.id) as uri,
      name_corporate_entity.sort_name as heading,
      'corporate_entity' as name_type,
      name_corporate_entity.source_id as name_source,
      name_authority_id.authority_id as authority_id,
      links.count as linked_records
      from agent_corporate_entity
        left outer join name_corporate_entity
          on name_corporate_entity.agent_corporate_entity_id = agent_corporate_entity.id
        left outer join name_authority_id
          on name_authority_id.name_corporate_entity_id = name_corporate_entity.id
        left outer join (
          select
            agent_corporate_entity.id as id,
            count(linked_agents_rlshp.id) as count
          from agent_corporate_entity
            left outer join linked_agents_rlshp
              on linked_agents_rlshp.agent_corporate_entity_id = agent_corporate_entity.id
          group by agent_corporate_entity.id
        ) as links
          on links.id = agent_corporate_entity.id
        left outer join user
          on user.agent_record_id = name_corporate_entity.agent_corporate_entity_id
    where name_corporate_entity.is_display_name
      and not name_corporate_entity.source_id is null
      and user.id is null)"
  end

  def identifier_field
    :sort_name
  end

  def page_break
    false
  end

end
