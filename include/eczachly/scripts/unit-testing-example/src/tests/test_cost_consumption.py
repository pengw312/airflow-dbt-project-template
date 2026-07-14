from pyspark.testing import assertDataFrameEqual
from ..jobs.course_consumption_job import do_course_consumption_job_transformation
from collections import namedtuple

import pytest
# events -  user_id, event_type, event_timestamp, ds (partition key)
# users - user_id, age, country, ds (partition key)
# aggregate output -  age, country, post_event_count, like_event_count, total_event_count, ds

ViewEvent = namedtuple('ViewEvent', 'user_id course_id duration_seconds ds')
User = namedtuple('User', 'user_id age country ds')
Course = namedtuple('Course', 'course_id ds')
Output = namedtuple('Output', 'user_id courses_viewed total_view_time total_view_events ds')

def test_course_jobs(spark):
    ds = "2023-01-01"

    view_events = spark.createDataFrame([
        ViewEvent(user_id=1, course_id=101, duration_seconds=300, ds="2023-01-01"),
        ViewEvent(user_id=1, course_id=102, duration_seconds=200, ds="2023-01-01"),
        ViewEvent(user_id=1, course_id=101, duration_seconds=100, ds="2023-01-01"),
        ViewEvent(user_id=2, course_id=101, duration_seconds=400, ds="2023-01-01"),

        # Should be ignored because ds is different
        ViewEvent(user_id=1, course_id=103, duration_seconds=999, ds="2023-01-02"),
    ])

    users = spark.createDataFrame([
        User(user_id=1, age=25, country="US", ds="2023-01-01"),
        User(user_id=2, age=30, country="DE", ds="2023-01-01"),

        # Should be ignored because ds is different
        User(user_id=3, age=40, country="FR", ds="2023-01-02"),
    ])

    courses = spark.createDataFrame([
        Course(course_id=101, ds="2023-01-01"),
        Course(course_id=102, ds="2023-01-01"),

        # Should be ignored because ds is different
        Course(course_id=103, ds="2023-01-02"),
    ])

    actual_df = do_course_consumption_job_transformation(
        spark=spark,
        courses=courses,
        users=users,
        view_events=view_events,
        ds=ds,
    )

    expected_df = spark.createDataFrame([
        Output(
            user_id=1,
            courses_viewed=2,
            total_view_time=600,
            total_view_events=3,
            ds="2023-01-01",
        ),
        Output(
            user_id=2,
            courses_viewed=1,
            total_view_time=400,
            total_view_events=1,
            ds="2023-01-01",
        ),
    ])

    assertDataFrameEqual(
        actual_df.orderBy("user_id"),
        expected_df.orderBy("user_id"),
    )
    