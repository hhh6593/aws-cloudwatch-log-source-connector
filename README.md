# aws-cloudwatch-log-source-connector
=================


AWS Cloudwatch Log Source Connector


-----

### Configuration


| name                        | data type | required | default      | description                        |
|:----------------------------|:----------|:---------|:-------------|:-----------------------------------|
| `aws.region`                | string    | yes      | -            | AWS Region                         |
| `aws.cloudwatch.log.group`  | string    | yes      | -            | 로그 그룹명                             |
| `aws.cloudwatch.log.stream` | string    | no       | -            | aws.cloudwatch.log.group 내 로그 스트림명 |
| `topic.name`                | string    | yes      | -            | 레코드를 전송할 토픽명                       |



Usage
-----

Build

    ./gradlew shadowJar


카프카 커넥트 런타임 plugin.path 추가 

    plugin.path={Source Connector Path}/build/libs

구성 예제(json)

    {
        "name": "aws-cloudwatch-log-source",
            "config": {
            "connector.class": "com.github.hans.AWSCloudwatchLogSourceConnector",
            "group.id": "aws-cloudwatch-source-group",
            "tasks.max": "1", 
            "aws.region": "ap-northeast-2",
            "aws.cloudwatch.log.group": "/ecs/test-api",
            "topic.name": "cld-test",
            "poll.interval.ms": 50000
       }
    }

Log Stream  
aws.cloudwatch.log.stream을 지정하지 않을 경우, aws.cloudwatch.log.group 내 가장 최신의 로그 스트림으로부터 레코드를 전송한다.

Credential  
AWS SDK의 자격 증명 공급자 체인을 사용하여 해당 정보를 탐색한다. 

Task  
1개의 태스크만을 사용하므로 tasks.max를 설정하더라도 항상 하나의 태스크만 생성된다.