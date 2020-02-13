import luigi
import profile_luigi


if __name__ == '__main__':
    required = [
        profile_luigi.ProfileAllColumns(),
    ]
    luigi.build(required,
                local_scheduler=True,
                workers=10,
                detailed_summary=True,
                log_level='INFO'
                )
