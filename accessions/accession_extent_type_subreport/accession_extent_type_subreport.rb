class AccessionExtentTypeSubreport < AbstractSubreport

  register_subreport('extent_type', ['accession'])

  def initialize(parent_report, accession_id, extent_type)
    super(parent_report)
    @accession_id = accession_id
    @extent_type = extent_type
  end

  def query_string
    "select
      sum(extent.number) as count
    from extent
      left outer join enumeration_value
        on enumeration_value.id = extent.extent_type_id
    where extent.accession_id = #{db.literal(@accession_id)}
      and enumeration_value.value = #{db.literal(@extent_type)}"
  end

  def self.field_name
    "#{@extent_type}"
  end
end
