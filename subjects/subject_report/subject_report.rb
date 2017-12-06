class SubjectReport < AbstractReport

  register_report

  def headers
    ['term', 'links', 'source', 'url', 'source_verified']
  end

  def processor
    {
      'source_verified' => proc {|record| verify_source(record[:source], record[:url])}
    }
  end

  def query
    db[:subject]
    .left_outer_join(:subject_rlshp, :subject_id => :subject__id)
    .left_outer_join(:enumeration_value, {:id => :subject__source_id}, :table_alias => :source)
      .select(Sequel.as(:subject__title, :term),
        Sequel.as(:source__value, :source),
        Sequel.as(:subject__authority_id, :url))
      .select_more{Sequel.as(count(:subject_rlshp__id), :links)}
      .group(:subject__id)
      .order(Sequel.asc(:term))
  end

  private

  def verify_source(source, url)
    locals = ["built", "local", "prov"]
    uri_prefix = {
      'aat' => "http://vocab.getty.edu/aat",
      'lcgft' => "http://id.loc.gov/authorities/genreForms",
      'lcnaf' => "http://id.loc.gov/authorities/names",
      'lcsh' => "http://id.loc.gov/authorities/subjects",
      'lcshg' => "http://id.loc.gov/authorities/subjects",
      'tgm' => "http://id.loc.gov/vocabulary/graphicMaterials",
      'tgn' => "http://vocab.getty.edu/tgn",
      'tucua' => "http://www2.archivists.org/thesaurus",
      'viaf' => "http://viaf.org/viaf"
    }

    if ASUtils.blank?(url)
      locals.include?(source) ? url : "Error: Authority ID must be present if a non-local source is declared"
    else
      if locals.include?(source)
        "Success"
      else
        prefix = uri_prefix[source]
        url.start_with?(prefix) ? "success" : "ERROR: Authority ID for #{source} must begin with #{prefix}"
      end
    end
  end
end
