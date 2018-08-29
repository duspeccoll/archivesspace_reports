class ResourceTermsSubreport < AbstractSubreport

  register_subreport('terms', ['resource'])

  def initialize(parent_report, resource_id)
    super(parent_report)
    @resource_id = resource_id
  end

  def query_string
    "select group_concat(subjects.term separator '\; ') as terms
    from (
      (select
        linked_agents_rlshp.resource_id as resource_id,
        name_person.sort_name as term
      from linked_agents_rlshp
        left outer join agent_person
          on agent_person.id = linked_agents_rlshp.agent_person_id
        left outer join name_person
          on name_person.agent_person_id = agent_person.id
        left outer join enumeration_value
          on enumeration_value.id = linked_agents_rlshp.role_id
      where linked_agents_rlshp.resource_id = #{db.literal(@resource_id)}
        and enumeration_value.value = 'subject'
        and name_person.is_display_name)

      union

      (select
        linked_agents_rlshp.resource_id as resource_id,
        name_corporate_entity.sort_name as term
      from linked_agents_rlshp
        left outer join agent_corporate_entity
          on agent_corporate_entity.id = linked_agents_rlshp.agent_corporate_entity_id
        left outer join name_corporate_entity
          on name_corporate_entity.agent_corporate_entity_id = agent_corporate_entity.id
        left outer join enumeration_value
          on enumeration_value.id = linked_agents_rlshp.role_id
      where linked_agents_rlshp.resource_id = #{db.literal(@resource_id)}
        and enumeration_value.value = 'subject'
        and name_corporate_entity.is_display_name)

      union

      (select
        linked_agents_rlshp.resource_id as resource_id,
        name_family.sort_name as term
      from linked_agents_rlshp
        left outer join agent_family
          on agent_family.id = linked_agents_rlshp.agent_family_id
        left outer join name_family
          on name_family.agent_family_id = agent_family.id
        left outer join enumeration_value
          on enumeration_value.id = linked_agents_rlshp.role_id
      where linked_agents_rlshp.resource_id = #{db.literal(@resource_id)}
        and enumeration_value.value = 'subject'
        and name_family.is_display_name)

      union

      (select
        subject_rlshp.resource_id as resource_id,
        subject.title as term
      from subject_rlshp
        left outer join subject
          on subject.id = subject_rlshp.subject_id
      where subject_rlshp.resource_id = #{db.literal(@resource_id)})
    ) as subjects
    group by subjects.resource_id"
  end

  def self.field_name
    'subjects'
  end

end
