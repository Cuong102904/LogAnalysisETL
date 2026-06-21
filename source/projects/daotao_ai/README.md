# daotao.ai source pack

`projects/daotao_ai` là source pack đầu tiên của LearnLake cho Open edX tracking logs.

Nó chịu trách nhiệm cho:

- routing rules trong [routing.yaml](/home/cuong/Desktop/DATN/source/projects/daotao_ai/routing.yaml)
- source-specific extractors trong [transforms.py](/home/cuong/Desktop/DATN/source/projects/daotao_ai/transforms.py)
- mapping assumptions và data dictionary cho Open edX fields

Các event groups chính đang map:

- `assessment`
- `video`
- `document`
- `navigation`
- `exam`
- `course_content`
- `authoring`
- `auth`
- `system`
- `unknown`

Ví dụ mapped events:

- `problem_check` browser/server
- `edx.grades.problem.submitted`
- `play_video`, `pause_video`, `seek_video`
- `book`, `textbook.pdf.*`
- `/api/edx_proctoring/...`
- `edx.ui.lms.link_clicked`
