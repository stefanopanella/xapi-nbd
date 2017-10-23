open Lwt.Infix

let section = Lwt_log.Section.make "Vbd_store"

module Make(Config : sig
    val vbd_list_dir : string
    val vbd_list_file_name : string
  end) = struct

  let vbd_list_dir = Config.vbd_list_dir
  let vbd_list_file_name = Config.vbd_list_file_name
  let vbd_list_file = vbd_list_dir ^ "/" ^ vbd_list_file_name

  let m = Lwt_mutex.create ()

  let log_and_reraise_error msg e =
    let%lwt () = Lwt_log.error_f ~section "%s: %s" msg (Printexc.to_string e) in
    Lwt.fail e

  let create_dir_if_doesnt_exist () =
    try%lwt
      Lwt_unix.mkdir vbd_list_dir 0o755
    with
    | Unix.(Unix_error (EEXIST, "mkdir", dir)) when dir = vbd_list_dir -> Lwt.return_unit

  let transform_vbd_list f =
    Lwt_mutex.with_lock m (fun () ->
        let%lwt () = create_dir_if_doesnt_exist () in
        (* We cannot have one stream here piped through a chain of functions,
           because the beginning of the stream (Lwt_io.lines_of_file) would read
           what the end of the stream writes (Lwt_io.lines_to_file), and it would
           overwrite the original file with duplicate entries. Instead, we read
           the whole stream into a list here to ensure the file gets closed. *)
        let%lwt l =
          try%lwt
            Lwt_io.lines_of_file vbd_list_file |> Lwt_stream.to_list
          with
          | Unix.(Unix_error (ENOENT, "open", file)) when file = vbd_list_file -> Lwt.return []
        in
        let l = f l in
        Lwt_stream.of_list l |> Lwt_io.lines_to_file vbd_list_file
      )

  let add vbd_uuid =
    transform_vbd_list (List.append [vbd_uuid])

  let remove vbd_uuid =
    transform_vbd_list (List.filter ((<>) vbd_uuid))

  let get_all () =
    (* Nothing should delete the vbd_list_file, so we do not have to use a
       Lwt.catch block here to prevent races where the file gets deleted after we
       check that it exists but before we use it. If it does get deleted, then it
       is fine to fail here, because that was done by an external program and is
       an error that the user/admin should fix. *)
    let%lwt exists = Lwt_unix.file_exists vbd_list_file in
    if exists then
      Lwt_mutex.with_lock m (fun () ->
          Lwt_io.lines_of_file vbd_list_file |> Lwt_stream.to_list
        )
    else
      Lwt.return []

end
