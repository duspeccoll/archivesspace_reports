class ArchivalObjectDigitizationReport < AbstractReport

  register_report

  def fix_row(row)
    ReportUtils.fix_boolean_fields(row, %i[representative])
  end

  def query_string
    "select
      concat('/repositories/#{@repo_id}/archival_objects/', archival_object.id) as uri,
      archival_object.title as title,
      archival_object.component_id as component_id,
      locations.location as location,
      digital_objects.url as url,
      digital_objects.representative as representative
    from archival_object
      left outer join (
        select
          archival_object_id as id,
          location as location
        from external_document
        where location is not null
          and title = 'Special Collections @ DU'
      ) as locations
        on locations.id = archival_object.id
      left outer join (
        select
          instance.archival_object_id as id,
          digital_object.digital_object_id as url,
          instance.is_representative as representative
        from instance
          left outer join instance_do_link_rlshp
            on instance_do_link_rlshp.instance_id = instance.id
          left outer join digital_object
            on digital_object.id = instance_do_link_rlshp.digital_object_id
      ) as digital_objects
        on digital_objects.id = archival_object.id
    where archival_object.repo_id = #{db.literal(@repo_id)}
      and locations.location is not null"
  end

end
